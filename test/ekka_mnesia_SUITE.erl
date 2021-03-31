%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Config.

end_per_suite(_Config) ->
    ok.

t_data_dir(_) ->
    ekka_mnesia:data_dir().

t_create_del_table(_) ->
    try
        application:ensure_all_started(ekka),
        ok = ekka_mnesia:create_table(kv_tab, [
                    {ram_copies, [node()]},
                    {record_name, kv_tab},
                    {attributes, record_info(fields, kv_tab)},
                    {storage_properties, []}]),
        ok = ekka_mnesia:copy_table(kv_tab, disc_copies),
        ok = mnesia:dirty_write(#kv_tab{key = a, val = 1}),
        {atomic, ok} = mnesia:del_table_copy(kv_tab, node())
    after
        application:stop(ekka),
        ekka_mnesia:ensure_stopped()
    end.

%% -spec(join_cluster(node()) -> ok).
%% -spec(leave_cluster(node()) -> ok | {error, any()}).
t_join_leave_cluster(_) ->
    Cluster = ekka_ct:cluster([core, core], []),
    try
        %% Implicitly causes N1 to join N0:
        [N0, N1] = ekka_ct:start_cluster(ekka, Cluster),
        ekka_ct:run_on(N0,
          fun() ->
                  #{running_nodes := [N0, N1]} = ekka_mnesia:cluster_info(),
                  [N0, N1] = lists:sort(ekka_mnesia:running_nodes()),
                  ok = rpc:call(N1, ekka_mnesia, leave_cluster, []),
                  #{running_nodes := [N0]} = ekka_mnesia:cluster_info(),
                  [N0] = ekka_mnesia:running_nodes()
          end)
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% -spec(cluster_status(node()) -> running | stopped | false).
t_cluster_status(_) ->
    Cluster = ekka_ct:cluster([core, core], []),
    try
        [N0, N1] = ekka_ct:start_cluster(ekka, Cluster),
        running = rpc:call(N0, ekka_mnesia, cluster_status, [N1])
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% -spec(remove_from_cluster(node()) -> ok | {error, any()}).
t_remove_from_cluster(_) ->
    Cluster = ekka_ct:cluster([core, core], []),
    try
        [N0, N1] = ekka_ct:start_cluster(ekka, Cluster),
        ekka_ct:run_on(N0, fun() ->
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
          end)
    after
        ok = ekka_ct:teardown_cluster(Cluster)
    end.

%% This test runs should walk the replicant state machine through all
%% the stages of startup and online transaction replication, so it can
%% be used to check if anything is _obviously_ broken.
t_rlog_smoke_test(_) ->
    snabbkaffe:fix_ct_logging(),
    Env = [{ekka, bootstrapper_chunk_config, #{count_limit => 3}}],
    Cluster = ekka_ct:cluster([core, core, replicant], Env),
    CounterKey = counter,
    ?check_trace(
       try
           %% Inject some orderings to make sure the replicant
           %% receives transactions in all states.
           %%
           %% 1. Commit some transactions before the replicant start:
           ?force_ordering(#{?snk_kind := trans_gen_counter_update, value := 5}, #{?snk_kind := state_change, to := disconnected}),
           %% 2. Make sure the rest of transactions are produced after the agent starts:
           ?force_ordering(#{?snk_kind := subscribe_realtime_stream}, #{?snk_kind := trans_gen_counter_update, value := 10}),
           %% 3. Make sure transactions are sent during TLOG replay: (TODO)
           ?force_ordering(#{?snk_kind := state_change, to := bootstrap}, #{?snk_kind := trans_gen_counter_update, value := 15}),
           %% 4. Make sure some transactions are produced while in normal mode
           ?force_ordering(#{?snk_kind := state_change, to := normal}, #{?snk_kind := trans_gen_counter_update, value := 25}),

           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           wait_shards([N1, N2], [test_shard]),
           %% Generate some transactions:
           {atomic, _} = rpc:call(N1, ekka_transaction_gen, init, []),
           ok = rpc:call(N1, ekka_transaction_gen, counter, [CounterKey, 30]),
           stabilize(1000), compare_table_contents(test_tab, Nodes),
           %% Create a delete transaction, to see if deletes are propagated too:
           {atomic, _} = rpc:call(N1, ekka_transaction_gen, delete, [1]),
           stabilize(1000), compare_table_contents(test_tab, Nodes),
           ekka_ct:stop_slave(N3),
           Nodes
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2, N3], Trace) ->
               %% Ensure that the nodes assumed designated roles:
               ?projection_complete(node, ?of_kind(rlog_server_start, Trace), [N1, N2]),
               ?projection_complete(node, ?of_kind(rlog_replica_start, Trace), [N3]),
               %% TODO: Check that some transactions have been buffered during catchup (to increase coverage):
               %?assertMatch([_|_], ?of_kind(rlog_replica_store_trans, Trace)),
               %% Other tests
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ?assert(ekka_rlog_props:all_batches_received(Trace)),
               ekka_rlog_props:counter_import_check(CounterKey, Trace)
       end).

t_transaction_on_replicant(_) ->
    Cluster = ekka_ct:cluster([core, replicant], []),
    ?check_trace(
       try
           Nodes = [N1, N2] = ekka_ct:start_cluster(ekka, Cluster),
           stabilize(1000),
           {atomic, _} = rpc:call(N2, ekka_transaction_gen, init, []),
           stabilize(1000), compare_table_contents(test_tab, Nodes),
           {atomic, KeyVals} = rpc:call(N2, ekka_transaction_gen, ro_read_all_keys, []),
           {atomic, KeyVals} = rpc:call(N1, ekka_transaction_gen, ro_read_all_keys, []),
           Nodes
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun([N1, N2], Trace) ->
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N2, Trace)),
               ?assert(ekka_rlog_props:all_batches_received(Trace))
       end).

cluster_benchmark(_) ->
    snabbkaffe:fix_ct_logging(),
    NReplicas = 6,
    Config = #{ trans_size => 10
              , max_time   => 15000
              , delays     => [10, 100, 1000]
              },
    ?check_trace(
       begin
           do_cluster_benchmark(Config#{ backend => mnesia
                                       , cluster => [core || I <- lists:seq(1, NReplicas)]
                                       }),
           do_cluster_benchmark(Config#{ backend  => ekka_mnesia
                                       , cluster => [core, core] ++ [replicant || I <- lists:seq(3, NReplicas)]
                                       })
       end,
       fun(_, _) ->
               snabbkaffe:analyze_statistics()
       end).

do_cluster_benchmark(#{ backend    := Backend
                      , trans_size := NKeys
                      , max_time   := MaxTime
                      , delays     := Delays
                      , cluster    := ClusterSpec
                      } = Config) ->
    Env = [ {ekka, rlog_rpc_module, rpc}
          ],
    Cluster = ekka_ct:cluster(ClusterSpec, Env),
    ResultFile = "/tmp/" ++ atom_to_list(Backend) ++ "_stats.csv",
    file:write_file( ResultFile
                   , ekka_ct:vals_to_csv([n_nodes | Delays])
                   ),
    [#{node := First}|_] = Cluster,
    try
        ekka_ct:start_cluster(node, Cluster),
        lists:foldl(
          fun(Node, Cnt) ->
                  ekka_ct:start_ekka(Node),
                  stabilize(100),
                  ok = rpc:call(First, ekka_transaction_gen, benchmark,
                                [ResultFile, Config, Cnt]),
                  Cnt + 1
          end,
          1,
          Cluster)
    after
        ekka_ct:teardown_cluster(Cluster)
    end.

wait_shards(Nodes, Shards) ->
    [{ok, _} = ?block_until(#{ ?snk_kind := "Shard fully up"
                             , shard     := Shard
                             , node      := Node
                             })
     || Shard <- Shards, Node <- Nodes],
    ok.

stabilize(Timeout) ->
    stabilize(Timeout, 10).

stabilize(_, 0) ->
    error(failed_to_stabilize);
stabilize(Timeout, N) ->
    case ?block_until(#{?snk_meta := [ekka, rlog|_]}, Timeout) of
        timeout -> ok;
        {ok, _} -> stabilize(Timeout, N - 1)
    end.

compare_table_contents(_, []) ->
    ok;
compare_table_contents(Table, Nodes) ->
    [{_, Reference}|Rest] = [{Node, lists:sort(rpc:call(Node, ets, tab2list, [Table]))}
                             || Node <- Nodes],
    lists:foreach(
      fun({Node, Contents}) ->
              ?assertEqual({Node, Reference}, {Node, Contents})
      end,
      Rest).
