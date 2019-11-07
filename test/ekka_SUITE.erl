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

-module(ekka_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("ekka.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    DataDir = proplists:get_value(data_dir, Config),
    Envs = proplists:get_value(ekka, generate_envs(DataDir)),
    [application:set_env(ekka, Par, Val) || {Par, Val} <- Envs],
    application:ensure_all_started(ekka),
    Config.

generate_envs(DataDir) ->
    Schema = cuttlefish_schema:files([filename:join([DataDir, "ekka.schema"])]),
    Config = conf_parse:file([filename:join([DataDir, "ekka.conf"])]),
    cuttlefish_generator:map(Schema, Config).

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% cluster group
%%--------------------------------------------------------------------

t_cluster(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    ekka_ct:wait_running(N1),
    true = ekka:is_running(N1, ekka),
    ok = rpc:call(N1, ekka_cluster, join, [N0]),
    [N1, N0] = lists:sort(mnesia:system_info(running_db_nodes)),
    ok = rpc:call(N1, ekka_cluster, leave, []),
    [N0] = lists:sort(mnesia:system_info(running_db_nodes)),
    ok = ekka_ct:stop_slave(N1).

t_cluster_join(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    N2 = ekka_ct:start_slave(ekka, n2),
    N3 = ekka_ct:start_slave(node, n3),
    ekka_ct:wait_running(N1),
    ekka_ct:wait_running(N2),
    true = ekka:is_running(N1, ekka),
    true = ekka:is_running(N2, ekka),
    %% case1
    ignore = ekka:join(N0),
    %% case2
    {error, {node_down, _}} = ekka:join(N3),
    %% case3
    ok = ekka:join(N1),
    Cluster = ekka_cluster:status(),
    [N1, N0] = proplists:get_value(running_nodes, Cluster),
    %% case4
    ok = ekka:join(N2),
    Cluster1 = ekka_cluster:status(),
    [N2, N0] = proplists:get_value(running_nodes, Cluster1),
    true = rpc:call(N1, ekka, is_running, [N1, ekka]),
    %% case4
    ok = rpc:call(N1, ekka, join, [N2]),
    ?assertEqual(3, length(proplists:get_value(running_nodes, ekka_cluster:status()))),
    %% case5
    ekka_ct:stop_slave(N1),
    timer:sleep(100),
    ?assertEqual(2, length(proplists:get_value(running_nodes, ekka_cluster:status()))),
    ?assertEqual(1, length(proplists:get_value(stopped_nodes, ekka_cluster:status()))),
    ekka_ct:stop_slave(N2),
    ekka_ct:stop_slave(N3).

t_cluster_leave(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    ekka_ct:wait_running(N1),
    {error, node_not_in_cluster} = ekka_cluster:leave(),
    ok = ekka_cluster:join(N1),
    [N1, N0] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:leave(),
    [N0] = ekka_mnesia:running_nodes(),
    ekka_ct:stop_slave(N1).

t_cluster_remove(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    ok = ekka_ct:wait_running(N1),
    ignore = ekka_cluster:force_leave(N0),
    ok = ekka_cluster:join(N1),
    [N1, N0] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:force_leave(N1),
    [N0] = ekka_mnesia:running_nodes(),
    ekka_ct:stop_slave(N0).

t_cluster_remove2(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    ok = emqx_ct:wait_running(N1),
    ok = ekka_cluster:join(N1),
    [N1, N0] = ekka_mnesia:running_nodes(),
    ok = ekka_cluster:force_leave(N1),
    ok = rpc:call(N1, ekka_mnesia, ensure_stopped, []),
    [N0] = ekka_mnesia:running_nodes(),
    ekka_ct:stop_slave(N1).

t_env(_) ->
    {ok, ekka} = ekka:env(cluster_name),
    {manual, []} = ekka:env(cluster_discovery).

t_callback(_) ->
    undefined = ekka:callback(shutdown).

t_autocluster(_) ->
    ekka:autocluster().

t_members(_) ->
    %% -spec(members() -> list(member())).
    [] = ekka:members().

t_local_member(_) ->
    %% -spec(local_member() -> member()).
    #member{node = Node, status = up} = ekka:local_member(),
    ?assertEqual(Node, node()).

t_is_member(_) ->
    %% -spec(is_member(node()) -> boolean()).
    ?assert(ekka:is_member(node())).

t_nodelist(_) ->
    %% -spec(nodelist() -> list(node())).
    ?assertEqual([node()], ekka:nodelist()).

t_status(_) ->
    %% [{members, members()}, {partitions, ekka_node_monitor:partitions()}].
    [{members, Members}, {partitions, []}] = ekka:status(),
    ?assertEqual(1, length(Members)).

t_is_aliving(_) ->
    %% -spec(is_aliving(node()) -> boolean()).
    ?assert(ekka:is_aliving(node())).

t_is_running(_) ->
    %% -spec(is_running(node(), atom()) -> boolean()).
    ?assert(ekka:is_running(node(), ekka)).

t_cluster_name(_) ->
    %% -spec(cluster_name() -> cluster()).
    ekka = ekka:cluster_name().

t_lock_unlock(_) ->
    {true, Nodes} = ekka:lock(resource),
    {true, Nodes} = ekka:unlock(resource),
    ?assertEqual(Nodes, [node()]).

