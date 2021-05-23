%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(ekka_cluster_etcd).

-behaviour(ekka_cluster_strategy).

-behaviour(gen_server).

-export([ discover/1
        , lock/1
        , unlock/1
        , register/1
        , unregister/1
        ]).

-export([start_link/1]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(SERVER, ?MODULE).

-record(state, {
    prefix,
    lease_id
}).

%% TTL callback
-export([etcd_set_node_key/1]).

%% for erlang apply
-export([ v2_discover/1
        , v2_lock/1
        , v2_unlock/1
        , v2_register/1
        , v2_unregister/1
        ]).

-export([ v3_discover/1
        , v3_lock/1
        , v3_unlock/1
        , v3_register/1
        , v3_unregister/1
        ]).

-define(LOG(Level, Format, Args), logger:Level("Ekka(etcd): " ++ Format, Args)).

start_link(Options) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%%--------------------------------------------------------------------
%% ekka_cluster_strategy callbacks
%%--------------------------------------------------------------------

discover(Options) ->
    etcd_apply(?FUNCTION_NAME, Options).

lock(Options) ->
    etcd_apply(?FUNCTION_NAME, Options).

unlock(Options) ->
    etcd_apply(?FUNCTION_NAME, Options).

register(Options) ->
    etcd_apply(?FUNCTION_NAME, Options).

unregister(Options) ->
    etcd_apply(?FUNCTION_NAME, Options).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
etcd_apply(Action, Options) ->
    case proplists:get_value(version, Options, v3) of
        v3 -> etcd_v3(Action);
        v2 -> etcd_v2(Action, Options)
    end.
%%--------------------------------------------------------------------
%% v2
%%--------------------------------------------------------------------
etcd_v2(Action, Options) ->
    Function = list_to_atom("v2_" ++ atom_to_list(Action)),
    erlang:apply(?MODULE, Function, [Options]).

v2_discover(Options) ->
    case etcd_get_nodes_key(Options) of
        {ok, Response} ->
            {ok, extract_nodes(Response)};
        {error, {404, _}} ->
            case ensure_nodes_path(Options) of
                {ok, _} -> discover(Options);
                Error -> Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

v2_lock(Options) ->
    v2_lock(Options, 10).
v2_lock(_Options, 0) ->
    {error, failed};
v2_lock(Options, Retries) ->
    case etcd_set_lock_key(Options) of
        {ok, _Response} -> ok;
        {error, {412, _}} ->
            timer:sleep(1000),
            v2_lock(Options, Retries -1);
        {error, Reason} ->
            {error, Reason}
    end.

v2_unlock(Options) ->
    case etcd_del_lock_key(Options) of
        {ok, _Response} -> ok;
        {error, Reason} ->
            {error, Reason}
    end.

v2_register(Options) ->
    case etcd_set_node_key(Options) of
        {ok, _Response} ->
            ensure_node_ttl(Options);
        {error, Reason} ->
            {error, Reason}
    end.

v2_unregister(Options) ->
    ok = ekka_cluster_sup:stop_child(ekka_node_ttl),
    case etcd_del_node_key(Options) of
        {ok, _Response} -> ok;
        {error, Reason} ->
            {error, Reason}
    end.

extract_nodes([]) ->
    [];
extract_nodes(Response) ->
    [extract_node(V) || V <- maps:get(<<"nodes">>, maps:get(<<"node">>, Response), [])].

ensure_node_ttl(Options) ->
    Ttl = proplists:get_value(node_ttl, Options),
    MFA = {?MODULE, etcd_set_node_key, [Options]},
    case ekka_cluster_sup:start_child(ekka_node_ttl, [Ttl, MFA]) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        Err = {error, _} -> Err
    end.

extract_node(V) ->
    list_to_atom(binary_to_list(lists:last(binary:split(maps:get(<<"key">>, V), <<"/">>, [global])))).

ensure_nodes_path(Options) ->
    etcd_set(server(Options), nodes_path(Options), [{dir, true}], ssl_options(Options)).

etcd_get_nodes_key(Options) ->
    etcd_get(server(Options), nodes_path(Options), [{recursive, true}], ssl_options(Options)).

etcd_set_node_key(Options) ->
    Ttl = config(node_ttl, Options),
    etcd_set(server(Options), node_path(Options), [{ttl, Ttl}], ssl_options(Options)).

etcd_del_node_key(Options) ->
    etcd_del(server(Options), node_path(Options), [], ssl_options(Options)).

etcd_set_lock_key(Options) ->
    Values = [{ttl, 30}, {'prevExist', false}, {value, node()}],
    etcd_set(server(Options), lock_path(Options), Values, ssl_options(Options)).

etcd_del_lock_key(Options) ->
    Values = [{'prevExist', true}, {'prevValue', node()}],
    etcd_del(server(Options), lock_path(Options), Values, ssl_options(Options)).

server(Options) ->
    config(server, Options).

ssl_options(Options) ->
    case proplists:get_value(ssl_options, Options, []) of
        [] -> [];
        SSLOptions -> [{ssl, SSLOptions}]
    end.

config(Key, Options) ->
    proplists:get_value(Key, Options).

etcd_get(Servers, Key, Params, HttpOpts) ->
    ekka_httpc:get(scheme(rand_addr(Servers)), Key, Params, HttpOpts).

etcd_set(Servers, Key, Params, HttpOpts) ->
    ekka_httpc:put(scheme(rand_addr(Servers)), Key, Params, HttpOpts).

etcd_del(Servers, Key, Params, HttpOpts) ->
    ekka_httpc:delete(scheme(rand_addr(Servers)), Key, Params, HttpOpts).

nodes_path(Options) ->
    with_prefix(config(prefix, Options), "/nodes").

node_path(Options) ->
    with_prefix(config(prefix, Options), "/nodes/" ++ atom_to_list(node())).

lock_path(Options) ->
    with_prefix(config(prefix, Options), "/lock").

with_prefix(Prefix, Path) ->
    Cluster = atom_to_list(ekka:env(cluster_name, ekka)),
    lists:concat(["v2/keys/", Prefix, "/", Cluster, Path]).

rand_addr([Addr]) ->
    Addr;
rand_addr(AddrList) ->
    lists:nth(rand:uniform(length(AddrList)), AddrList).

%%--------------------------------------------------------------------
%% v3
%%--------------------------------------------------------------------
etcd_v3(Action) ->
    gen_server:call(?SERVER, Action, 5000).

v3_discover(#state{prefix = Prefix}) ->
    Context = v3_nodes_context(Prefix),
    case eetcd_kv:get(Context) of
        {ok, Response} ->
            case maps:get(kvs, Response) of
                [] ->
                    {ok, []};
                KvsList ->
                    Nodes = [
                        binary_to_atom(maps:get(value, Kvs), utf8) || Kvs <- KvsList],
                    {ok, Nodes}
            end;
        Error ->
            Error
    end.

v3_lock(#state{prefix = Prefix, lease_id = ID}) ->
    case eetcd_lock:lock(?MODULE, list_to_binary(v3_lock_key(Prefix)), ID) of
        {ok, #{key := LockKey}} ->
            persistent_term:put(ekka_cluster_etcd_lock_key, LockKey),
            ok;
        Error ->
            Error
    end.

v3_unlock(_) ->
    case persistent_term:get(ekka_cluster_etcd_lock_key, undefined) of
        undefined ->
            {error, lock_lose};
        LockKey ->
            case eetcd_lock:unlock(?MODULE, LockKey) of
                {ok, _} ->
                    persistent_term:erase(ekka_cluster_etcd_lock_key),
                    ok;
                Error ->
                    Error
            end
    end.

v3_register(#state{prefix = Prefix ,lease_id = ID}) ->
    Context = v3_node_context(Prefix, ID),
    case eetcd_kv:put(Context) of
        {ok, _Response} ->
            ok;
        Error ->
            Error
    end.

v3_unregister(#state{prefix = Prefix}) ->
    Context = v3_node_context_only_key(Prefix),
    case eetcd_kv:delete(Context) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

v3_nodes_context(Prefix) ->
    Ctx = eetcd_kv:new(?MODULE),
    Ctx1 = eetcd_kv:with_key(Ctx, v3_nodes_key(Prefix)),
    Ctx2 = eetcd_kv:with_range_end(Ctx1, "\0"),
    eetcd_kv:with_sort(Ctx2, 'KEY', 'ASCEND').

v3_node_context(Prefix, ID) ->
    Ctx = eetcd_kv:new(?MODULE),
    Ctx1 = eetcd_kv:with_key(Ctx, v3_node_key(Prefix)),
    Ctx2 = eetcd_kv:with_value(Ctx1, atom_to_binary(node(), utf8)),
    eetcd_kv:with_lease(Ctx2, ID).

v3_node_context_only_key(Prefix) ->
    Ctx = eetcd_kv:new(?MODULE),
    eetcd_kv:with_key(Ctx, v3_node_key(Prefix)).

v3_lock_key(Prefix) ->
    Prefix ++ "/ekkacl/lock/".

v3_nodes_key(Prefix) ->
    Prefix ++ "/ekkacl/nodes/".

v3_node_key(Prefix) ->
    v3_node_key(Prefix, atom_to_list(node())).

v3_node_key(Prefix, Node) ->
    v3_nodes_key(Prefix) ++ Node.

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init(Options) ->
    process_flag(trap_exit, true),
    Servers = proplists:get_value(server, Options, []),
    Prefix = proplists:get_value(prefix, Options),
    Hosts = [remove_scheme(Server) || Server <- Servers],
    {Transport, TransportOpts} = case ssl_options(Options) of
        [] -> {tcp, []};
        [SSL] -> SSL
    end,
    {ok, _Pid} = eetcd:open(?MODULE, Hosts, Transport, TransportOpts),
    {ok, #{'ID' := ID}} = eetcd_lease:grant(?MODULE, 5),
    {ok, _Pid2} = eetcd_lease:keep_alive(?MODULE, ID),
    {ok, #state{prefix = Prefix, lease_id = ID}}.

handle_call(Action, _From, State) when is_atom(Action) ->
    Function = list_to_atom("v3_" ++ atom_to_list(Action)),
    Reply = erlang:apply(?MODULE, Function, [State]),
    {reply, Reply, State};

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({'EXIT', _From, Reason}, State) ->
    eetcd:close(?MODULE),
    {stop, Reason, State};

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

remove_scheme("http://" ++ Url) ->
    Url;
remove_scheme("https://" ++ Url) ->
    Url;
remove_scheme(Url) ->
    Url.

scheme("http://" ++ _ = Url) ->
    Url;
scheme("https://" ++ _ = Url) ->
    Url;
scheme(Url) ->
    "http://" ++ Url.
