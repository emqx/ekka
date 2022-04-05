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

-module(ekka_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-record(kv_tab, {key, val}).

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_name, ekka),
    ok = application:set_env(ekka, cluster_discovery, {manual, []}),
    application:ensure_all_started(ekka),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(ekka),
    ekka_mnesia:ensure_stopped().

t_data_dir(_) ->
    ekka_mnesia:data_dir().

t_create_del_table(_) ->
    ok = ekka_mnesia:create_table(kv_tab, [
                {ram_copies, [node()]},
                {record_name, kv_tab},
                {attributes, record_info(fields, kv_tab)},
                {storage_properties, []}]),
    ok = ekka_mnesia:copy_table(kv_tab, disc_copies),
    ok = mnesia:dirty_write(#kv_tab{key = a, val = 1}),
    {atomic, ok} = mnesia:del_table_copy(kv_tab, node()).

%% -spec(join_cluster(node()) -> ok).
%% -spec(leave_cluster(node()) -> ok | {error, any()}).
t_join_leave_cluster(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(node, n1),
    try
        ok = rpc:call(N1, ekka_mnesia, start, []),
        ok = rpc:call(N1, ekka_mnesia, join_cluster, [N0]),
        #{running_nodes := [N0, N1]} = ekka_mnesia:cluster_info(),
        [N0, N1] = lists:sort(ekka_mnesia:running_nodes()),
        ok = rpc:call(N1, ekka_mnesia, leave_cluster, []),
        #{running_nodes := [N0]} = ekka_mnesia:cluster_info(),
        [N0] = ekka_mnesia:running_nodes(),
        ok = rpc:call(N1, ekka_mnesia, ensure_stopped, [])
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%% -spec(cluster_status(node()) -> running | stopped | false).
t_cluster_status(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(node, n1),
    try
        ok = rpc:call(N1, ekka_mnesia, start, []),
        ok = rpc:call(N1, ekka_mnesia, join_cluster, [N0]),
        running = ekka_mnesia:cluster_status(N1),
        ok = rpc:call(N1, ekka_mnesia, leave_cluster, [])
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%% -spec(remove_from_cluster(node()) -> ok | {error, any()}).
t_remove_from_cluster(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(node, n1),
    try
        ok = rpc:call(N1, ekka_mnesia, start, []),
        ok = rpc:call(N1, ekka_mnesia, join_cluster, [N0]),
        #{running_nodes := [N0, N1]} = ekka_mnesia:cluster_info(),
        [N0, N1] = lists:sort(ekka_mnesia:running_nodes()),
        [N0, N1] = lists:sort(ekka_mnesia:cluster_nodes(all)),
        [N0, N1] = lists:sort(ekka_mnesia:cluster_nodes(running)),
        [] = ekka_mnesia:cluster_nodes(stopped),
        ok = ekka_mnesia:remove_from_cluster(N1),
        #{running_nodes := [N0]} = ekka_mnesia:cluster_info(),
        [N0] = ekka_mnesia:running_nodes(),
        [N0] = ekka_mnesia:cluster_nodes(all),
        [N0] = ekka_mnesia:cluster_nodes(running),
        ok = rpc:call(N1, ekka_mnesia, ensure_stopped, [])
    after
        ok = ekka_ct:stop_slave(N1)
    end.

t_diagnosis_tab(_)->
    TestTab = test_tab_1,
    N1 = ekka_ct:start_slave(node, n1),
    N2 = ekka_ct:start_slave(node, n2),

    try
        %% Start ekka on two nodes
        ok = rpc:call(N1, ekka_mnesia, start, []),
        ok = rpc:call(N2, ekka_mnesia, start, []),
        ok = rpc:call(N1, ekka_mnesia, join_cluster, [N2]),

        %% Create a test table
        {atomic, ok} = rpc:call(N1, mnesia, create_table, [TestTab, [{disc_copies, [N1,N2]}]]),
        %% Ensure table is ready
        ?assertEqual(ok, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        timer:sleep(100),
        %% Stop N1
        ok = ekka_ct:stop_slave(N1),
        %% Stop N2
        ok = ekka_ct:stop_slave(N2),
        ?assertEqual({badrpc, nodedown}, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual({badrpc, nodedown}, rpc:call(N2, mnesia, wait_for_tables, [[TestTab], 1000])),

        %% Start N1, N1 mnesia doesn't know N2 is down
        N1 = ekka_ct:start_slave(node, n1),
        ok = rpc:call(N1, mnesia, start, []), %%ekka_mnesia:start/0 will block
        ?assertEqual( {timeout,[test_tab_1]}
                    , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 5000])),
        ?assertEqual(ok, rpc:call(N1, ekka_mnesia, diagnosis, [[TestTab]])),

        %% Start N2 only, but not mnesia
        N2 = ekka_ct:start_slave(node, n2),
        %% Check N1 still waits for the mnesia on N2
        ?assertEqual( {timeout,[test_tab_1]}
                    , rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 5000])),
        ?assertEqual(ok, rpc:call(N1, ekka_mnesia, diagnosis, [[TestTab]])),

        %% Start mnesia on N2.
        ok = rpc:call(N2, ekka_mnesia, start, []),

        %% Start ekka_mnesia on N1, no blocking
        ok = rpc:call(N1, ekka_mnesia, start, []),

        %% Check tables are loaded on two
        ?assertEqual(ok, rpc:call(N1, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual(ok, rpc:call(N2, mnesia, wait_for_tables, [[TestTab], 1000])),
        ?assertEqual(ok, rpc:call(N1, ekka_mnesia, diagnosis, [[TestTab]])),
        ?assertEqual(ok, rpc:call(N2, ekka_mnesia, diagnosis, [[TestTab]]))

    after
        ?assertEqual({atomic, ok}, rpc:call(N2, mnesia, delete_table, [TestTab])),

        ok = ekka_ct:stop_slave(N1),
        ok = ekka_ct:stop_slave(N2)
    end,
    ok.
