%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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

-module(ekka_SUITE).

-compile(export_all).

-include("ekka.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

all() ->
     [{group, cluster}].

groups() ->
    [{cluster, [sequence],
     [cluster_test,
      cluster_join,
      cluster_leave,
      cluster_remove,
      cluster_remove2
     ]}].

init_per_suite(Config) ->
    NewConfig = generate_config(),
    Vals = proplists:get_value(ekka, NewConfig),
    [application:set_env(ekka, Par, Value) || {Par, Value} <- Vals],
    application:ensure_all_started(ekka),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% cluster group
%%--------------------------------------------------------------------
cluster_test(_Config) ->
    Z = slave(ekka, cluster_test_z),
    wait_running(Z),
    true = ekka:is_running(Z, ekka),
    Node = node(),
    ok  = rpc:call(Z, ekka_cluster, join, [Node]),
    [Z, Node] = lists:sort(mnesia:system_info(running_db_nodes)),
    ok = rpc:call(Z, ekka_cluster, leave, []),
    [Node] = lists:sort(mnesia:system_info(running_db_nodes)),
    ok = slave:stop(Z).

cluster_join(_Config) ->
    Z = slave(ekka, cluster_join_z),
    Z1 = slave(ekka, cluster_join_z1),
    N = slave(node, cluster_join_n),
    wait_running(Z),
    true = ekka:is_running(Z, ekka),
    true = ekka:is_running(Z1, ekka),
    Node = node(),
    %% case1
    ignore = ekka:join(Node),
    %% case2
    {error, {node_down, _}} = ekka:join(N),
    %% case3
    ok = ekka:join(Z),
    Cluster = ekka_cluster:status(),
    [Z, Node] = proplists:get_value(running_nodes, Cluster),
    %% case4
    ok = ekka:join(Z1),
    Cluster1 = ekka_cluster:status(),
    [Z1, Node] = proplists:get_value(running_nodes, Cluster1),
    true = rpc:call(Z, ekka, is_running, [Z, ekka]),
    %% case4
    ok = rpc:call(Z, ekka, join, [Z1]),
    ?assertEqual(3, length(proplists:get_value(running_nodes, ekka_cluster:status()))),
    %% case5
    slave:stop(Z),
    timer:sleep(100),
    ?assertEqual(2, length(proplists:get_value(running_nodes, ekka_cluster:status()))),
    ?assertEqual(1, length(proplists:get_value(stopped_nodes, ekka_cluster:status()))),
    slave:stop(N),
    slave:stop(Z1).

cluster_leave(_Config) ->
    Z = slave(ekka, cluster_leave_z),
    wait_running(Z),
    {error, node_not_in_cluster} = ekka_cluster:leave(),
    ok = ekka_cluster:join(Z),
    Node = node(),
    [Z, Node] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:leave(),
    [Node] = ekka_mnesia:running_nodes(),
    slave:stop(Z).

cluster_remove(_Config) ->
    Z = slave(ekka, cluster_remove_z),
    wait_running(Z),
    Node = node(),
    ignore = ekka_cluster:force_leave(Node),
    ok = ekka_cluster:join(Z),
    [Z, Node] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:force_leave(Z),
    [Node] = ekka_mnesia:running_nodes(),
    slave:stop(Z).

cluster_remove2(_Config) ->
    Z = slave(ekka, cluster_remove2_z),
    wait_running(Z),
    ok = ekka_cluster:join(Z),
    Node = node(),
    [Z, Node] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:force_leave(Z),
    ok = rpc:call(Z, ekka_mnesia, ensure_stopped, []),
    [Node] = ekka_mnesia:running_nodes(),
    slave:stop(Z).

host() -> [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc:call(Node, ekka, is_running, [Node, ekka]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

slave(ekka, Node) ->
    {ok, Ekka} = slave:start(host(), Node, ensure_slave()),
    rpc:call(Ekka, application, ensure_all_started, [ekka]),
    Ekka;

slave(node, Node) ->
    {ok, N} = slave:start(host(), Node, ensure_slave()),
    N.

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "ekka.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "ekka.conf.example"])]),
    cuttlefish_generator:map(Schema, Conf).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

ensure_slave() ->
    EbinDir = local_path(["ebin"]),
    DepsDir = local_path(["deps", "*", "ebin"]),
    "-pa " ++ EbinDir ++ " -pa " ++ DepsDir.
