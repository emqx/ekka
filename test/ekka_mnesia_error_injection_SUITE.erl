%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Tests that use error injection should go here, to avoid polluting
%% the logs and scaring people
-module(ekka_mnesia_error_injection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").


all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    ekka_ct:cleanup(TestCase),
    snabbkaffe:stop(),
    Config.

t_agent_restart(_) ->
    Cluster = ekka_ct:cluster([core, core, replicant], ekka_mnesia_test_util:common_env()),
    CounterKey = counter,
    ?check_trace(
       try
           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           ekka_mnesia_test_util:stabilize(1000),
           %% Everything in ekka agent will crash
           ?inject_crash( #{?snk_meta := #{domain := [ekka, rlog, agent|_]}}
                        , snabbkaffe_nemesis:random_crash(0.4)
                        ),
           ok = rpc:call(N1, ekka_transaction_gen, counter, [CounterKey, 100, 100]),
           ekka_mnesia_test_util:stabilize(5100),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           N3
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(N3, Trace) ->
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ekka_rlog_props:counter_import_check(CounterKey, N3, Trace),
               ?assert(length(?of_kind(snabbkaffe_crash, Trace)) > 1)
       end).

t_rand_error_injection(_) ->
    Cluster = ekka_ct:cluster([core, core, replicant], ekka_mnesia_test_util:common_env()),
    CounterKey = counter,
    ?check_trace(
       try
           Nodes = [N1, N2, N3] = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           ekka_mnesia_test_util:stabilize(1000),
           %% Everything in ekka RLOG will crash
           ?inject_crash( #{?snk_meta := #{domain := [ekka, rlog|_]}}
                        , snabbkaffe_nemesis:random_crash(0.01)
                        ),
           ok = rpc:call(N1, ekka_transaction_gen, counter, [CounterKey, 300, 100]),
           ekka_mnesia_test_util:stabilize(5100),
           ekka_mnesia_test_util:compare_table_contents(test_tab, Nodes),
           N3
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(N3, Trace) ->
               ?assert(ekka_rlog_props:replicant_bootstrap_stages(N3, Trace)),
               ?assert(ekka_rlog_props:counter_import_check(CounterKey, N3, Trace) > 0)
       end).

%% This testcase verifies verifies various modes of ekka_mnesia:ro_transaction
t_sum_verify(_) ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    NTrans = 100,
    ?check_trace(
       try
           Nodes = ekka_ct:start_cluster(ekka, Cluster),
           ekka_mnesia_test_util:wait_shards(Nodes),
           %% Everything in ekka RLOG will crash
           ?inject_crash( #{?snk_meta := #{domain := [ekka, rlog|_]}}
                        , snabbkaffe_nemesis:random_crash(0.1)
                        ),
           [rpc:async_call(N, ekka_transaction_gen, verify_trans_sum, [NTrans, 100])
            || N <- lists:reverse(Nodes)],
           [?block_until(#{?snk_kind := verify_trans_sum, node := N})
            || N <- Nodes]
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun(_, Trace) ->
               ?assertMatch( [ok, ok]
                           , ?projection(result, ?of_kind(verify_trans_sum, Trace))
                           )
       end).
