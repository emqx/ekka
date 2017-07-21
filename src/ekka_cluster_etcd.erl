%%%===================================================================
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(ekka_cluster_etcd).

-behaviour(ekka_cluster_strategy).

-export([discover/1, lock/1, unlock/1, register/1, unregister/1]).

%% Ttl callback
-export([etcd_set_node_key/1]).

-define(LOG(Level, Format, Args),
        lager:Level("Ekka(etcd): " ++ Format, Args)).

%%%===================================================================
%%% ekka_cluster_strategy Callbacks
%%%===================================================================

discover(Options) ->
    NodesPath = nodes_path(config(prefix, Options)),
    case etcd_get(config(server, Options), NodesPath, [{recursive, true}]) of
        {ok, Response} ->
            extract_nodes(Response);
        {error, Error} ->
            ?LOG(error, "Discovery error - ~p", [Error]), []
    end.

lock(Options) ->
    lock(Options, 10).

lock(_Options, 0) ->
    {error, failed};

lock(Options, Retries) ->
    case etcd_set_lock_key(Options) of
        {ok, Response} ->
            ?LOG(debug, "Lock Response: ~p", [Response]),
            ok;
        {error, {412, _}} ->
            timer:sleep(1000),
            lock(Options, Retries -1);
        {error, Reason} ->
            ?LOG(error, "Lock Error: ~p", [Reason]),
            {error, Reason}
    end.

unlock(Options) ->
    case etcd_del_lock_key(Options) of
        {ok, Response} ->
            ?LOG(debug, "Unlock Response: ~p", [Response]),
            ok;
        {error, Reason} ->
            ?LOG(error, "Unlock Error: ~p", [Reason]),
            {error, Reason}
    end.

register(Options) ->
    case etcd_set_node_key(Options) of
        {ok, Response} ->
            ?LOG(debug, "Register Response: ~p", [Response]),
            Ttl = proplists:get_value(node_ttl, Options),
            MFA = {?MODULE, etcd_set_node_key, [Options]},
            {ok, _Pid} = ekka_cluster_sup:start_child(ekka_node_ttl, [Ttl, MFA]),
            ok;
        {error, Reason} ->
            ?LOG(error, "Register error - ~p", [Reason]),
            {error, Reason}
    end.

unregister(Options) ->
    ok = ekka_cluster_sup:stop_child(ekka_node_ttl),
    case etcd_del_node_key(Options) of
        {ok, Response} ->
            ?LOG(debug, "Unregister Response: ~p", [Response]),
            ok;
        {error, Reason} ->
            ?LOG(error, "Unregister error - ~p", [Reason]),
            {error, Reason}
    end.

etcd_set_node_key(Options) ->
    NodePath = node_path(config(prefix, Options)),
    Ttl = config(node_ttl, Options) div 1000,
    etcd_set(config(server, Options), NodePath, [{ttl, Ttl}]).

etcd_set_lock_key(Options) ->
    LockPath = lock_path(config(prefix, Options)),
    Values = [{ttl, 30}, {'prevExist', false}, {value, node()}],
    etcd_set(config(server, Options), LockPath, Values).

etcd_del_node_key(Options) ->
    NodePath = node_path(config(prefix, Options)),
    etcd_del(config(server, Options), NodePath, []).

etcd_del_lock_key(Options) ->
    LockPath = lock_path(config(prefix, Options)),
    Values = [{'prevExist', true}, {'prevValue', node()}],
    etcd_del(config(server, Options), LockPath, Values).

%%%===================================================================
%%% Internal functions
%%%===================================================================

config(Key, Options) ->
    proplists:get_value(Key, Options).

nodes_path(Prefix) ->
    with_prefix(Prefix, "/nodes").

node_path(Prefix) ->
    with_prefix(Prefix, "/nodes/" ++ atom_to_list(node())).

lock_path(Prefix) ->
    with_prefix(Prefix, "/lock").

extract_nodes([]) ->
    [];
extract_nodes(Response) ->
    [extract_node(V) || V <- maps:get(<<"nodes">>, maps:get(<<"node">>, Response), [])].

extract_node(V) ->
    list_to_atom(binary_to_list(lists:last(binary:split(maps:get(<<"key">>, V), <<"/">>, [global])))).

etcd_get(Servers, Key, Params) ->
    ekka_httpc:get(rand_addr(Servers), Key, Params).

etcd_set(Servers, Key, Params) ->
    ekka_httpc:put(rand_addr(Servers), Key, Params).

etcd_del(Servers, Key, Params) ->
    ekka_httpc:delete(rand_addr(Servers), Key, Params).

with_prefix(Prefix, Path) ->
    "v2/keys/" ++ Prefix ++ "/" ++ atom_to_list(ekka:env(cluster_name, ekka)) ++ Path.

rand_addr([Addr]) ->
    Addr;
rand_addr(AddrList) ->
    lists:nth(rand:uniform(length(AddrList)), AddrList).

