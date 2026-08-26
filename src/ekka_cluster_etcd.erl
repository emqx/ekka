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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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

%% TTL of the etcd v3 lease that the node key is attached to. The
%% keepalive process refreshes the lease; if the node dies, etcd
%% deletes the node key when the lease expires.
-define(LEASE_TTL_SECONDS, 5).

%% Reconnect backoff: 1s, 2s, 4s, ... capped at 30s.
-define(RECONNECT_BASE_MS, 1000).
-define(RECONNECT_MAX_MS, 30000).
-define(RECONNECT, reconnect).

%% Whether the node key should exist in etcd; used to restore the key
%% after a reconnect or a process restart (the old lease expired and
%% etcd deleted the key). Kept in a persistent_term so that it
%% survives a restart of this process.
-define(REGISTERED_KEY, ekka_cluster_etcd_registered).

-record(state, {
    prefix,
    %% undefined when the connection to etcd is down
    lease_id :: pos_integer() | undefined,
    keepalive_pid :: pid() | undefined,
    hosts = [] :: list(),
    open_opts = [] :: list(),
    retries = 0 :: non_neg_integer()
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
    ?tp(ekka_cluster_etcd_v2_register, #{}),
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
        SSLOptions ->
            case proplists:get_value(enable, SSLOptions, true) of
                true -> [{ssl, proplists:delete(enable, SSLOptions)}];
                false -> []
            end
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
    Timeout = case Action of
                  %% etcd would keep a dangling lock if we don't wait for it
                  lock -> infinity;
                  %% sligthly higher than the default eetcd timeout
                  _ -> 10000
              end,
    gen_server:call(?SERVER, Action, Timeout).

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
    Context = eetcd:with_timeout(eetcd:new(?MODULE), infinity),
    Name = list_to_binary(v3_lock_key(Prefix)),
    Context1 = eetcd_lock:with_lease(eetcd_lock:with_name(Context, Name), ID),
    case eetcd_lock:lock(Context1) of
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
    ?tp(ekka_cluster_etcd_v3_register, #{prefix => Prefix, lease_id => ID}),
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
    OpenOpts = case ssl_options(Options) of
        [] -> [{transport, tcp}];
        [{ssl, TLSOpts}] -> [{transport, tls}, {tls_opts, TLSOpts}]
    end,
    State = #state{prefix = Prefix, hosts = Hosts, open_opts = OpenOpts},
    case connect(State) of
        {ok, State1} ->
            %% restore the node key if the previous incarnation of this
            %% process had registered it
            {ok, ensure_registered(State1)};
        {error, Reason} ->
            ?LOG(warning, "failed to connect to etcd server(s) ~p: ~p",
                 [Hosts, Reason]),
            {ok, schedule_reconnect(State)}
    end.

handle_call(Action, _From, State) when is_atom(Action) ->
    {reply, handle_action(Action, State), State};

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(?RECONNECT, State = #state{lease_id = ID}) when ID =/= undefined ->
    %% stale timer message; already connected
    {noreply, State};

handle_info(?RECONNECT, State) ->
    case connect(State) of
        {ok, State1} ->
            ?LOG(info, "reconnected to etcd", []),
            ?tp(ekka_cluster_etcd_reconnected, #{}),
            {noreply, ensure_registered(State1)};
        {error, Reason} ->
            ?tp(ekka_cluster_etcd_reconnect_failed, #{reason => Reason}),
            ?LOG(warning, "failed to connect to etcd: ~p", [Reason]),
            {noreply, schedule_reconnect(State)}
    end;

handle_info({'EXIT', Pid, Reason}, State = #state{keepalive_pid = Pid}) ->
    %% The lease keepalive process halts when the connection to etcd is
    %% lost. Disconnect and reconnect with backoff instead of stopping:
    %% a stop would make the supervisor restart this process, and with
    %% etcd still down the restarts would exhaust the supervisor's
    %% restart intensity, killing cluster discovery for good.
    ?tp(ekka_cluster_etcd_keepalive_halted, #{reason => Reason}),
    ?LOG(warning, "etcd lease keepalive halted: ~p", [Reason]),
    {noreply, schedule_reconnect(disconnect(State))};

handle_info({'EXIT', _From, Reason}, State) ->
    %% exit signal from a process other than the lease keepalive
    {stop, Reason, State};

handle_info(#{event := 'KeepAliveHalted'}, State) ->
    %% informational message from eetcd_lease; the 'EXIT' of the linked
    %% keepalive process drives the reconnect
    {noreply, State};

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, #state{lease_id = undefined}) ->
    _ = eetcd:close(?MODULE),
    ok;
terminate(_Reason, #state{lease_id = ID}) ->
    _ = eetcd_lease:revoke(?MODULE, ID),
    _ = eetcd:close(?MODULE),
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

handle_action(Action, #state{lease_id = undefined}) ->
    %% No connection to etcd. Callers (ekka_autocluster, mria core node
    %% discovery) log the error and retry on their own timers.
    note_registered(Action, error),
    {error, etcd_disconnected};
handle_action(Action, State) ->
    Function = list_to_atom("v3_" ++ atom_to_list(Action)),
    Reply = erlang:apply(?MODULE, Function, [State]),
    note_registered(Action, Reply),
    Reply.

%% Track whether the node key should exist in etcd, so that a
%% reconnect or a process restart can restore it after the old lease
%% expired.
note_registered(register, ok) ->
    persistent_term:put(?REGISTERED_KEY, true);
note_registered(unregister, _) ->
    persistent_term:put(?REGISTERED_KEY, false);
note_registered(_Action, _Reply) ->
    ok.

is_registered() ->
    persistent_term:get(?REGISTERED_KEY, false).

connect(State = #state{hosts = Hosts, open_opts = OpenOpts}) ->
    %% At the time of writing, the etcd connection process does not
    %% close when this process dies.  So, when this processes is
    %% restarted by its supervisor, the `eetcd:open' call fails with
    %% `{error,[{{"localhost",2379},already_started}]}'.  This ensures
    %% that no connection with this name exists before opening it
    %% (again).
    _ = eetcd:close(?MODULE),
    case eetcd:open(?MODULE, Hosts, OpenOpts) of
        {ok, _Pid} ->
            grant_lease(State);
        {error, Reason} ->
            {error, {failed_to_connect, Reason}}
    end.

grant_lease(State) ->
    case eetcd_lease:grant(?MODULE, ?LEASE_TTL_SECONDS) of
        {ok, #{'ID' := ID}} ->
            start_keepalive(State#state{lease_id = ID});
        {error, Reason} ->
            _ = eetcd:close(?MODULE),
            {error, {failed_to_grant_lease, Reason}}
    end.

start_keepalive(State = #state{lease_id = ID}) ->
    case eetcd_lease:keep_alive(?MODULE, ID) of
        {ok, Pid} ->
            true = link(Pid),
            {ok, State#state{keepalive_pid = Pid, retries = 0}};
        {error, Reason} ->
            _ = eetcd:close(?MODULE),
            {error, {failed_to_start_keepalive, Reason}}
    end.

%% The node key was attached to the expired lease, so etcd deleted it
%% during the disconnect. ekka_autocluster stops itself once discovery
%% completes, so nothing else re-creates the key: re-register it here.
ensure_registered(State) ->
    case is_registered() andalso v3_register(State) of
        false ->
            State;
        ok ->
            ?tp(ekka_cluster_etcd_reregistered, #{}),
            State;
        {error, Reason} ->
            ?LOG(warning, "failed to re-register node in etcd: ~p", [Reason]),
            schedule_reconnect(disconnect(State))
    end.

disconnect(State = #state{keepalive_pid = Pid}) ->
    case is_pid(Pid) of
        true ->
            unlink(Pid),
            %% flush an already-delivered exit signal, if any
            receive {'EXIT', Pid, _} -> ok after 0 -> ok end;
        false ->
            ok
    end,
    _ = eetcd:close(?MODULE),
    State#state{lease_id = undefined, keepalive_pid = undefined}.

schedule_reconnect(State = #state{retries = Retries}) ->
    Delay = reconnect_delay(Retries),
    _ = erlang:send_after(Delay, self(), ?RECONNECT),
    State#state{retries = Retries + 1}.

reconnect_delay(Retries) ->
    min(?RECONNECT_MAX_MS, ?RECONNECT_BASE_MS bsl min(Retries, 6)).

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
