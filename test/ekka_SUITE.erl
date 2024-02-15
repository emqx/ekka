%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-compile(nowarn_deprecated_function). %% Silence the warnings about slave module

-include_lib("mria/include/mria.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> ekka_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    snabbkaffe:fix_ct_logging(),
    application:set_env(gen_rpc, port_discovery, stateless),
    Config.

set_env({Par, Val}) ->
    application:set_env(ekka, Par, Val).

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    DataDir = proplists:get_value(data_dir, Config),
    AppConfig = filename:join([DataDir, "ekka.config"]),
    {ok, Envs} = file:consult(AppConfig),
    ok = lists:foreach(fun set_env/1, Envs),
    application:load(ekka),
    ekka:start(),
    Config.

end_per_testcase(TestCase, Config) ->
    ekka_ct:cleanup(TestCase),
    Config.

%%--------------------------------------------------------------------
%% Env
%%--------------------------------------------------------------------

t_env(_) ->
    {ok, ekka} = ekka:env(cluster_name),
    {ok, {manual, []}} = ekka:env(cluster_discovery),
    default = ekka:env(badkey, default).

%%--------------------------------------------------------------------
%% CT for cluster
%%--------------------------------------------------------------------

t_cluster_name(_) ->
    ekka = ekka:cluster_name().

t_cluster_manually(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        true = ekka:is_running(N1, ekka),
        ok = rpc:call(N1, ekka, join, [N0]),
        [N0, N1] = ekka:info(running_nodes),
        ok = rpc:call(N1, ekka, leave, []),
        [N0] = ekka:info(running_nodes)
    after
        ok = ekka_ct:stop_slave(N1)
    end.


t_join_leave_cluster(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    N2 = ekka_ct:start_slave(ekka, n2),
    N3 = ekka_ct:start_slave(node, n3),
    try
        ok = ekka_ct:wait_running(N1),
        ok = ekka_ct:wait_running(N2),
        true = ekka:is_running(N1, ekka),
        true = ekka:is_running(N2, ekka),
        false = ekka:is_running(N3, ekka),
        %% Create cluster
        ignore = ekka:join(N0),
        {error, {node_down, _}} = ekka:join(N3),
        ok = rpc:call(N1, ekka, join, [N0]),
        [N0, N1] = ekka:info(running_nodes),
        ok = rpc:call(N2, ekka, join, [N0]),
        [N0, N1, N2] = ekka:info(running_nodes),
        %% Restart N1
        ok = slave:stop(N1),
        ?retry(1000, 10,
               [N0, N2] = ekka:info(running_nodes)),
        [N1] = ekka:info(stopped_nodes),
        N1 = ekka_ct:start_slave(ekka, n1),
        ?retry(1000, 30,
               [N0, N1, N2] = ekka:info(running_nodes)),
        %% Force Leave
        ok = ekka:force_leave(N1),
        ok = ekka:force_leave(N2),
        ?retry(1000, 10,
               [N0] = ekka:info(running_nodes))
    after
        ok = ekka_ct:stop_slave(N1),
        ok = ekka_ct:stop_slave(N2),
        ok = ekka_ct:stop_slave(N3)
    end.

t_cluster_force_leave(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = rpc:call(N1, ekka, join, [N0]),
        [N0, N1] = ekka:info(running_nodes),
        ignore = ekka:force_leave(N0),
        ok = ekka:force_leave(N1),
        [N0] = ekka:info(running_nodes)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

t_cluster_force_leave2(_Config) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = rpc:call(N1, ekka, join, [N0]),
        [N0, N1] = ekka:info(running_nodes),
        ok = ekka:force_leave(N1),
        _ = rpc:call(N1, mria, stop, []),
        [N0] = ekka:info(running_nodes)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

t_autocluster(_) ->
    ekka:autocluster().

t_is_aliving(_) ->
    ?assertMatch(true, ekka:is_aliving(node())),
    N1 = ekka_ct:start_slave(node, n1),
    try
        ?assertMatch(true, ekka:is_aliving(N1))
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%% -spec(is_running(node(), atom()) -> boolean()).
t_is_running(_) ->
    true = ekka:is_running(node(), ekka).

t_nodelist(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
      ok = ekka_ct:wait_running(N1),
      ok = rpc:call(N1, ekka, join, [N0]),
      [N0, N1] = lists:sort(ekka:nodelist(up)),
      ok = rpc:call(N1, ekka, leave, []),
      ?retry(100, 100,
             [N0] = lists:sort(ekka:nodelist()))
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%% -spec(local_member() -> member()).
t_local_member(_) ->
    N = node(),
    #member{node = N, status = up} = ekka:local_member().

t_is_member(_) ->
    true = ekka:is_member(node()),
    N1 = ekka_ct:start_slave(node, n1),
    try
        false = ekka:is_member(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

t_lock_unlock(_) ->
    {true, Nodes} = ekka:lock(resource),
    {true, Nodes} = ekka:unlock(resource),
    ?assertEqual(Nodes, [node()]).

t_lock_unlock_all(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = rpc:call(N1, ekka, join, [N0]),
        {true, Nodes} = ekka:lock(resource, all),
        {true, Nodes} = ekka:unlock(resource, all),
        {true, Nodes} = ekka:lock(resource, all),
        {true, Nodes} = ekka:unlock(resource, all),
        ?assertEqual([N0, N1], lists:sort(Nodes)),
        ok = rpc:call(N1, ekka, leave, [])
    after
        ok = ekka_ct:stop_slave(N1)
    end.
