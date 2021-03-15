%%--------------------------------------------------------------------
%% Copyright (c) 2019-2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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

t_async_cluster_start(_) ->
    Cluster = [ {core, n1}
              , {core, n2}
              , {replicant, n3}
              ],
    Env = [ {shards, [foo]}
          , {rlog_rpc_fun, fun rpc:call/4}
          ],
    ?check_trace(
       #{timeout => 5000},
       begin
           Nodes = [N1, N2, N3] = ekka_ct:cluster(Cluster, Env),
           wait_shards(Nodes, [foo]),
           {atomic, _} = rpc:call(N1, ekka_transaction_gen, create_keys, []),
           Nodes
       end,
       fun([N1, N2, N3], Trace) ->
               %% Ensure that the nodes assumed designated roles:
               ?projection_complete(node, ?of_kind(rlog_server_start, Trace), [N1, N2]),
               ?projection_complete(node, ?of_kind(rlog_replica_start, Trace), [N3]),
               %% Other tests
               replicant_bootstrap_stages(N3, Trace),
               error(no_way)
       end).

replicant_bootstrap_stages(Node, Trace) ->
    Transitions = [To || #{ ?snk_kind := state_change
                          , ?snk_meta := #{node := Node, domain := [ekka, rlog, replica]}
                          , to := To
                          } <- Trace],
    ?assertMatch( [disconnected, bootstrap, local_replay, normal]
                , Transitions
                ).

wait_shards(Nodes, Shards) ->
    [{ok, _} = ?block_until(#{ ?snk_kind := "Shard fully up"
                             , shard     := Shard
                             , node      := Node
                             })
     || Shard <- Shards, Node <- Nodes],
    ok.
