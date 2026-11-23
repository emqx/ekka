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

-module(ekka_autoheal_SUITE).

-include_lib("snabbkaffe/include/test_macros.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = init_app_envs(node()),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    ok = ekka:stop(),
    ekka_mnesia:ensure_stopped().

t_enabled(_Config) ->
    {true, 2000} = ekka_autoheal:enabled().

t_autoheal(_Config) ->
    [N1,N2,N3] = Nodes = lists:map(fun start_slave_node/1, [n1,n2,n3]),
    try
        %% Create cluster
        ok = rpc:call(N2, ekka, join, [N1]),
        ok = rpc:call(N3, ekka, join, [N1]),
        ?check_trace(
            begin
                %% Simulate netsplit
                true = rpc:cast(N3, net_kernel, disconnect, [N1]),
                true = rpc:cast(N3, net_kernel, disconnect, [N2]),
                ok = timer:sleep(1000),
                %% SplitView: {[N1,N2], [N3]}
                [N1,N2] = rpc:call(N1, ekka, info, [running_nodes]),
                [N3] = rpc:call(N1, ekka, info, [stopped_nodes]),
                [N1,N2] = rpc:call(N2, ekka, info, [running_nodes]),
                [N3] = rpc:call(N2, ekka, info, [stopped_nodes]),
                [N3] = rpc:call(N3, ekka, info, [running_nodes]),
                [N1,N2] = rpc:call(N3, ekka, info, [stopped_nodes]),
                %% Simulate autoheal crash, to verify autoheal tolerates it.
                snabbkaffe_nemesis:inject_crash(
                    ?match_event(#{?snk_kind := "Healing cluster partition"}),
                    snabbkaffe_nemesis:recover_after(1),
                    ?MODULE
                ),
                %% Wait for autoheal
                ok = timer:sleep(12000),
                [N1,N2,N3] = rpc:call(N1, ekka, info, [running_nodes]),
                [N1,N2,N3] = rpc:call(N2, ekka, info, [running_nodes]),
                [N1,N2,N3] = rpc:call(N3, ekka, info, [running_nodes]),
                rpc:call(N1, ekka, leave, []),
                rpc:call(N2, ekka, leave, []),
                rpc:call(N3, ekka, leave, [])
            end,
            fun(Trace) ->
                ?assertMatch(
                    [#{need_reboot := [N3]}],
                    ?of_kind("Healing cluster partition", Trace)
                ),
                ?assertMatch([_ | _], ?of_kind(snabbkaffe_crash, Trace))
            end
        )
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

t_autoheal_asymm(_Config) ->
    [N1,N2,N3,N4] = Nodes = lists:map(fun start_slave_node/1, [ah1,ah2,ah3,ah4]),
    try
        %% Create cluster
        ok = rpc:call(N2, ekka, join, [N1]),
        ok = rpc:call(N3, ekka, join, [N1]),
        ok = rpc:call(N4, ekka, join, [N1]),
        ?check_trace(
            begin
                %% Simulate asymmetric netsplit
                true = rpc:cast(N2, net_kernel, disconnect, [N1]),
                true = rpc:cast(N3, net_kernel, disconnect, [N1]),
                true = rpc:cast(N4, net_kernel, disconnect, [N2]),
                ok = timer:sleep(1000),
                %% Asymmetric split, but it's enough to reboot N1 and N2
                NodesInfo = [running_nodes, stopped_nodes],
                [[N1,N4], [N2,N3]] = [rpc:call(N1, ekka, info, [I]) || I <- NodesInfo],
                [[N2,N3], [N1,N4]] = [rpc:call(N2, ekka, info, [I]) || I <- NodesInfo],
                [[N2,N3,N4], [N1]] = [rpc:call(N3, ekka, info, [I]) || I <- NodesInfo],
                [[N1,N3,N4], [N2]] = [rpc:call(N4, ekka, info, [I]) || I <- NodesInfo],
                %% Wait for autoheal
                ok = timer:sleep(12000),
                Nodes = rpc:call(N1, ekka, info, [running_nodes]),
                Nodes = rpc:call(N2, ekka, info, [running_nodes]),
                Nodes = rpc:call(N3, ekka, info, [running_nodes]),
                Nodes = rpc:call(N4, ekka, info, [running_nodes]),
                rpc:call(N1, ekka, leave, []),
                rpc:call(N2, ekka, leave, []),
                rpc:call(N3, ekka, leave, []),
                rpc:call(N4, ekka, leave, [])
            end,
            fun(Trace) ->
                ?assertMatch(
                    [#{need_reboot := [N1, N2, N3, N4]}],
                    ?of_kind("Healing cluster partition", Trace)
                )
            end
        )
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

t_autoheal_fullsplit(_Config) ->
    [N1,N2,N3,N4] = Nodes = lists:map(fun start_slave_node/1, [fs1,fs2,fs3,fs4]),
    try
        %% Create cluster
        ok = rpc:call(N2, ekka, join, [N1]),
        ok = rpc:call(N3, ekka, join, [N1]),
        ok = rpc:call(N4, ekka, join, [N1]),
        ?check_trace(
            begin
                %% Simulate asymmetric netsplit
                true = rpc:cast(N1, net_kernel, disconnect, [N2]),
                true = rpc:cast(N1, net_kernel, disconnect, [N3]),
                true = rpc:cast(N1, net_kernel, disconnect, [N4]),
                true = rpc:cast(N2, net_kernel, disconnect, [N3]),
                true = rpc:cast(N2, net_kernel, disconnect, [N4]),
                true = rpc:cast(N3, net_kernel, disconnect, [N4]),
                ok = timer:sleep(1000),
                %% Full split, all nodes except one need to be rebooted
                NodesInfo = [running_nodes, stopped_nodes],
                [[N1], [N2,N3,N4]] = [rpc:call(N1, ekka, info, [I]) || I <- NodesInfo],
                [[N2], [N1,N3,N4]] = [rpc:call(N2, ekka, info, [I]) || I <- NodesInfo],
                [[N3], [N1,N2,N4]] = [rpc:call(N3, ekka, info, [I]) || I <- NodesInfo],
                [[N4], [N1,N2,N3]] = [rpc:call(N4, ekka, info, [I]) || I <- NodesInfo],
                %% Wait for autoheal
                ok = timer:sleep(12000),
                Nodes = rpc:call(N1, ekka, info, [running_nodes]),
                Nodes = rpc:call(N2, ekka, info, [running_nodes]),
                Nodes = rpc:call(N3, ekka, info, [running_nodes]),
                Nodes = rpc:call(N4, ekka, info, [running_nodes]),
                rpc:call(N1, ekka, leave, []),
                rpc:call(N2, ekka, leave, []),
                rpc:call(N3, ekka, leave, []),
                rpc:call(N4, ekka, leave, [])
            end,
            fun(Trace) ->
                ?assertMatch(
                    [#{need_reboot := [N2, N3, N4]}],
                    ?of_kind("Healing cluster partition", Trace)
                )
            end
        )
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

%%--------------------------------------------------------------------
%% A partition report must survive the leader's Ekka being temporarily down:
%% the reporter keeps re-confirming and addresses the leader elected by then.
%%--------------------------------------------------------------------

t_confirm_retry_after_leader_ekka_restart(_Config) ->
    run_leader_restart_scenario(false).

t_confirm_retry_suppressed_after_clear(_Config) ->
    run_leader_restart_scenario(true).

run_leader_restart_scenario(ClearBeforeRecovery) ->
    [L, P, R] = Nodes = lists:map(fun start_slave_node/1, [pcr_l, pcr_p, pcr_r]),
    try
        ok = rpc:call(P, ekka, join, [L]),
        ok = rpc:call(R, ekka, join, [L]),
        ok = wait_until(fun() -> leader_agrees(Nodes, L) end, 100),
        Test = self(),
        ok = watch_reports(P, Test),
        ok = watch_confirm_targets(R, Test),
        %% R confirms the partition against L, which is still the leader.
        true = rpc:cast(R, erlang, send,
                        [ekka_node_monitor,
                         {mnesia_system_event, {inconsistent_database, undefined, P}}]),
        ok = wait_until(fun() -> rpc:call(R, ekka_node_monitor, partitions, []) =:= [P] end, 100),
        ok = rpc:call(L, application, stop, [ekka]),
        %% Wait for the first confirmation to observe L as unavailable.
        receive {is_running, R, L, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
        case ClearBeforeRecovery of
            true ->
                true = rpc:cast(R, erlang, send,
                                [ekka_node_monitor, {mnesia_system_event, {mnesia_up, P}}]),
                ok = wait_until(fun() -> rpc:call(R, ekka_node_monitor, partitions, []) =:= [] end, 100);
            false ->
                ok
        end,
        %% L restarts its Ekka and gets a new GUID, so P becomes the leader.
        ok = rpc:call(L, ekka, start, []),
        ok = wait_until(fun() -> leader_agrees(Nodes, P) end, 100),
        case ClearBeforeRecovery of
            true ->
                receive
                    {autoheal_msg, P, {report_partition, R}} ->
                        ct:fail(reported_after_partition_cleared)
                after 6000 -> ok
                end,
                receive {cluster_view, P} -> ct:fail(planned_after_partition_cleared)
                after 0 -> ok
                end,
                receive {is_running, R, _, _} -> ct:fail(confirmed_after_partition_cleared)
                after 0 -> ok
                end;
            false ->
                receive
                    {autoheal_msg, P, {report_partition, R}} -> ok
                after 10000 -> ct:fail(no_report_to_new_leader)
                end,
                receive {cluster_view, P} -> ok after 5000 -> ct:fail(no_plan_after_report) end
        end
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes)
    end.

leader_agrees(Nodes, Leader) ->
    lists:all(fun(N) -> rpc:call(N, ekka_membership, leader, []) =:= Leader end, Nodes).

watch_reports(Node, Test) ->
    ok = rpc:call(Node, meck, new, [ekka_autoheal, [passthrough, no_link]]),
    ok = rpc:call(Node, meck, expect, [ekka_autoheal, handle_msg,
        fun(Msg, Autoheal) ->
            Test ! {autoheal_msg, node(), Msg},
            meck:passthrough([Msg, Autoheal])
        end]),
    ok = rpc:call(Node, meck, new, [ekka_mnesia, [passthrough, no_link]]),
    ok = rpc:call(Node, meck, expect, [ekka_mnesia, cluster_view,
        fun() ->
            Test ! {cluster_view, node()},
            meck:passthrough([])
        end]).

watch_confirm_targets(Node, Test) ->
    ok = rpc:call(Node, meck, new, [ekka_node, [passthrough, no_link]]),
    ok = rpc:call(Node, meck, expect, [ekka_node, is_running,
        fun(Target, App) ->
            Result = meck:passthrough([Target, App]),
            Test ! {is_running, node(), Target, Result},
            Result
        end]).

start_slave_node(Name) ->
    Node = ekka_ct:start_slave(node, Name),
    ok = init_app_envs(Node),
    ok = rpc:call(Node, ekka, start, []),
    ok = ekka_ct:wait_running(Node),
    true = ekka:is_running(Node, ekka),
    Node.

init_app_envs(N) ->
    _ = rpc:call(N, application, load, [ekka]),
    Config = [{ekka, [{cluster_name, ekka},
                      {cluster_enable, true},
                      {cluster_autoheal, 2000},
                      {cluster_discovery, {manual, []}}
                     ]
              }],
    rpc:call(N, application, set_env, [Config]).

%%--------------------------------------------------------------------
%% heal_node/1 single-rpc unit tests
%%--------------------------------------------------------------------

%% Verifies that ekka_autoheal:heal_node/1 invokes ekka_cluster:heal/1 with
%% the single atom shutdown_and_reboot (single-rpc path) and does not crash.
%% The slave runs the real ekka_cluster, so the clause must exist and return ok.
t_heal_single_rpc(_Config) ->
    Node = start_slave_node(sr1),
    try
        %% Direct rpc validates the clause exists and returns ok.
        %% heal(shutdown_and_reboot) on a standalone slave:
        %%   prepare(heal) -> announce + application:stop(ekka)
        %%   reboot()      -> ekka:start()
        %% The slave ends up running ekka again.
        ok = rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 30000),
        ok = ekka_ct:wait_running(Node),
        true = ekka:is_running(Node, ekka),
        %% Invoke heal_node/1 proper and verify it completes without crash.
        %% Return value is not contractual; we only care that it doesn't raise.
        _ = ekka_autoheal:heal_node(Node),
        ok = ekka_ct:wait_running(Node),
        true = ekka:is_running(Node, ekka)
    after
        ekka_ct:stop_slave(Node)
    end.

%% Verifies that a 2-node cluster is fully re-joined at mnesia level after
%% one peer executes heal(shutdown_and_reboot). This is a regression guard:
%% an earlier draft of heal/1 that omitted ekka_mnesia:ensure_stopped/started
%% left both nodes alive with only self in running_nodes (i.e. mnesia schema
%% never re-joined the cluster).
t_heal_restores_mnesia_running_nodes(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [mr1, mr2]),
    try
        %% Form a 2-node cluster.
        ok = rpc:call(N2, ekka, join, [N1]),
        true = lists:sort(rpc:call(N1, ekka_mnesia, running_nodes, [])) =:=
               lists:sort(Nodes),
        true = lists:sort(rpc:call(N2, ekka_mnesia, running_nodes, [])) =:=
               lists:sort(Nodes),
        %% Have N2 perform the atomic heal locally.
        %% Success contract: after this call, mnesia schema on N2 has
        %% re-joined the cluster (not running as a detached singleton).
        ok = rpc:call(N2, ekka_cluster, heal, [shutdown_and_reboot], 30000),
        ok = ekka_ct:wait_running(N2),
        true = ekka:is_running(N2, ekka),
        %% Both nodes must see each other in mnesia running_nodes again.
        ?assertEqual(lists:sort(Nodes),
                     lists:sort(rpc:call(N1, ekka_mnesia, running_nodes, []))),
        ?assertEqual(lists:sort(Nodes),
                     lists:sort(rpc:call(N2, ekka_mnesia, running_nodes, [])))
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes)
    end.

%% Integration test: full autoheal GenServer path on a 2-node cluster
%% ends with mnesia cluster fully re-joined on BOTH nodes.
%%
%% This is the end-to-end guard for the regression where an earlier draft
%% of heal(shutdown_and_reboot) skipped the ekka_mnesia stop/start pair
%% and left the victim's mnesia schema as a detached singleton after the
%% autoheal completed (ekka layer looked healthy, but
%% ekka_mnesia:running_nodes/0 returned just self on each node).
%%
%% Trigger is net_kernel:disconnect (not a direct ekka_cluster:heal/1 call),
%% so the autoheal GenServer drives the whole chain:
%%   partition detected -> split view computed -> coordinator picks victim
%%   -> rpc to victim's ekka_cluster:heal(shutdown_and_reboot)
%%   -> victim runs prepare(heal), ensure_stopped, ensure_started, reboot
%%   -> mnesia schema re-joins the cluster peer
%%
%% Failure mode if regression returns: ekka running_nodes eventually shows
%% both, but ekka_mnesia:running_nodes/0 stays at [self()] on each side.
t_autoheal_restores_mnesia(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [ahm1, ahm2]),
    try
        %% Form a 2-node cluster and sanity-check both layers are in sync.
        ok = rpc:call(N2, ekka, join, [N1]),
        ?assertEqual(lists:sort(Nodes),
                     lists:sort(rpc:call(N1, ekka_mnesia, running_nodes, []))),
        ?assertEqual(lists:sort(Nodes),
                     lists:sort(rpc:call(N2, ekka_mnesia, running_nodes, []))),
        ?check_trace(
            begin
                %% Symmetric 2-node netsplit. Autoheal picks a coordinator
                %% deterministically (the node with the lexicographically
                %% smaller running-nodes list) and rpcs heal into the peer.
                true = rpc:cast(N1, net_kernel, disconnect, [N2]),
                ok = timer:sleep(1000),
                %% Each side now sees only itself.
                [N1] = rpc:call(N1, ekka, info, [running_nodes]),
                [N2] = rpc:call(N1, ekka, info, [stopped_nodes]),
                [N2] = rpc:call(N2, ekka, info, [running_nodes]),
                [N1] = rpc:call(N2, ekka, info, [stopped_nodes]),
                %% Wait for the autoheal GenServer to drive the full chain.
                ok = timer:sleep(12000),
                %% Ekka membership view recovered on both nodes.
                ?assertEqual(lists:sort(Nodes),
                             lists:sort(rpc:call(N1, ekka, info, [running_nodes]))),
                ?assertEqual(lists:sort(Nodes),
                             lists:sort(rpc:call(N2, ekka, info, [running_nodes]))),
                %% Mnesia cluster view also recovered on both nodes.
                %% This is the specific assertion that would fail if the
                %% ensure_stopped/started pair were missing from
                %% heal(shutdown_and_reboot).
                ?assertEqual(lists:sort(Nodes),
                             lists:sort(rpc:call(N1, ekka_mnesia, running_nodes, []))),
                ?assertEqual(lists:sort(Nodes),
                             lists:sort(rpc:call(N2, ekka_mnesia, running_nodes, [])))
            end,
            fun(Trace) ->
                ?assertMatch(
                    [#{need_reboot := [_]} | _],
                    ?of_kind("Healing cluster partition", Trace)
                )
            end
        )
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

%% Verifies that heal_node/1 uses a 30s timeout (not infinity) on every rpc:call.
%% A live-timeout test would burn 30s of CI wall-clock, so we inspect the
%% source to ensure the bounded timeout is present and infinity is absent
%% within the heal_node/1 function body.
t_heal_timeout_30s(_Config) ->
    SrcPath = locate_ekka_autoheal_src(),
    {ok, Bin} = file:read_file(SrcPath),
    Src = binary_to_list(Bin),
    FunText = extract_heal_node_body(Src),
    %% Must contain bounded 30s timeout on rpc:call.
    ?assertNotEqual(nomatch, string:find(FunText, ", 30000)")),
    %% Must NOT use infinity timeout anywhere in heal_node/1.
    ?assertEqual(nomatch, string:find(FunText, "infinity")),
    ok.

locate_ekka_autoheal_src() ->
    {ok, Cwd} = file:get_cwd(),
    Candidates0 = [
        filename:join([Cwd, "src", "ekka_autoheal.erl"]),
        filename:join([Cwd, "..", "src", "ekka_autoheal.erl"]),
        filename:join([Cwd, "..", "..", "src", "ekka_autoheal.erl"]),
        filename:join([Cwd, "..", "..", "..", "src", "ekka_autoheal.erl"]),
        filename:join([Cwd, "..", "..", "..", "..", "src", "ekka_autoheal.erl"])
    ],
    Candidates = case code:lib_dir(ekka) of
                     {error, _} -> Candidates0;
                     LibDir     -> Candidates0 ++
                                   [filename:join([LibDir, "src",
                                                   "ekka_autoheal.erl"])]
                 end,
    case [P || P <- Candidates, filelib:is_regular(P)] of
        [Found | _] -> Found;
        []          -> error({ekka_autoheal_src_not_found, Candidates})
    end.

extract_heal_node_body(Src) ->
    Marker = "heal_node(Node) ->",
    case string:find(Src, Marker) of
        nomatch ->
            error(heal_node_marker_not_found);
        Tail ->
            %% Stop at the next top-level form: a blank line followed by a
            %% non-whitespace character (start of another function or clause
            %% group). "\n\n" is the boundary used throughout this module.
            case string:find(Tail, "\n\n") of
                nomatch -> Tail;
                Rest    ->
                    Len = string:length(Tail) - string:length(Rest),
                    string:slice(Tail, 0, Len)
            end
    end.

t_heal_does_not_fallback_on_internal_function_clause(_Config) ->
    Node = start_slave_node(heal_internal_clause),
    try
        Test = self(),
        Prepare = fun(Action) ->
            Test ! {preparing, node()},
            prepare_join_only(Action)
        end,
        ok = rpc:call(Node, ekka, callback, [prepare, Prepare]),
        ok = ekka_node_monitor:cast(node(), {heal_cluster, [Node], []}),
        receive {preparing, Node} -> ok after 5000 -> error(heal_not_started) end,
        receive {preparing, Node} -> error(heal_replayed) after 3000 -> ok end,
        ?assert(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

prepare_join_only(join) -> ok.

%% Poll Fun/0 until it returns true or Retries attempts have elapsed.
%% Sleep 50ms between attempts. Returns ok on success, errors on timeout.
wait_until(_Fun, 0) ->
    error({wait_until, timeout});
wait_until(Fun, Retries) when Retries > 0 ->
    case Fun() of
        true  -> ok;
        _     -> timer:sleep(50), wait_until(Fun, Retries - 1)
    end.

%%--------------------------------------------------------------------
%% A heal refused by the membership guard must be re-confirmed and retried; the retries
%% are bounded.
%%--------------------------------------------------------------------

t_autoheal_retries_rejected_heal(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [ah_retry1, ah_retry2]),
    try
        ok = rpc:call(N2, ekka, join, [N1]),
        ?assertEqual(lists:sort(Nodes), lists:sort(rpc:call(N1, ekka_mnesia, running_nodes, []))),
        [ok = install_test_guard(N, first_rejected) || N <- Nodes],
        true = rpc:cast(N1, net_kernel, disconnect, [N2]),
        ?assertEqual(ok, wait_until(fun() -> split_observed(Nodes) end, 100)),
        %% No second partition event is created: the retry must come from the rejected heal.
        ?assertEqual(ok, wait_until(fun() -> joined(Nodes) end, 400)),
        %% One rejected admission, one successful admission and the application start.
        ?assert(lists:max([guard_calls(N) || N <- Nodes]) >= 3)
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

t_autoheal_retry_is_bounded(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [ah_bound1, ah_bound2]),
    try
        ok = rpc:call(N2, ekka, join, [N1]),
        [ok = install_test_guard(N, always_rejected) || N <- Nodes],
        true = rpc:cast(N1, net_kernel, disconnect, [N2]),
        ?assertEqual(ok, wait_until(fun() -> lists:max([guard_calls(N) || N <- Nodes]) >= 3 end, 400)),
        ok = timer:sleep(3000),
        %% The heal is given up after the bounded attempts; no further attempts are made.
        ?assertEqual([0, 3], lists:sort([guard_calls(N) || N <- Nodes])),
        ?assertNotEqual(Nodes, rpc:call(N1, ekka_mnesia, running_nodes, []))
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

t_invalid_initial_view_is_retried(_Config) ->
    Invalid = fun(Attempt) -> Attempt =:= 1 end,
    with_view_failure([ah_initial1, ah_initial2], Invalid, fun(Nodes, Faulty) ->
        Monitors = monitors(Nodes),
        report_split(Nodes),
        wait_for_invalid_view(Faulty, 1),
        ?assertEqual(Monitors, monitors(Nodes)),
        ?assertEqual([0, 0], [guard_calls(N) || N <- Nodes]),
        ?assertEqual(ok, wait_until(fun() -> joined(Nodes) end, 400))
    end).

t_invalid_retry_view_is_retried(_Config) ->
    Invalid = fun(Attempt) -> Attempt =:= 2 end,
    with_view_failure([ah_retry_view1, ah_retry_view2], Invalid, fun(Nodes, Faulty) ->
        report_split(Nodes),
        wait_for_invalid_view(Faulty, 2),
        ?assertEqual([0, 1], lists:sort([guard_calls(N) || N <- Nodes])),
        ?assertEqual(ok, wait_until(fun() -> joined(Nodes) end, 400))
    end).

t_invalid_retry_views_exhaust_budget(_Config) ->
    Invalid = fun(Attempt) -> Attempt >= 2 end,
    with_view_failure([ah_invalid1, ah_invalid2], Invalid, fun(Nodes, Faulty) ->
        Monitors = monitors(Nodes),
        report_split(Nodes),
        wait_for_invalid_view(Faulty, 2),
        wait_for_invalid_view(Faulty, 3),
        receive {invalid_view, Faulty, Attempt} -> error({unexpected_view_read, Attempt})
        after 3000 -> ok
        end,
        ?assertEqual([0, 1], lists:sort([guard_calls(N) || N <- Nodes])),
        ?assertEqual(Monitors, monitors(Nodes)),
        ?assert(split_observed(Nodes))
    end).

with_view_failure(Names, Reject, Test) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, Names),
    try
        ok = rpc:call(N2, ekka, join, [N1]),
        [ok = install_test_guard(N, first_rejected) || N <- Nodes],
        ok = rpc:call(N2, meck, new, [ekka_mnesia, [passthrough, no_link]]),
        Parent = self(),
        View = fun() ->
            Key = {?MODULE, view_reads},
            Attempt = application:get_env(ekka, Key, 0) + 1,
            ok = application:set_env(ekka, Key, Attempt),
            case Reject(Attempt) of
                true ->
                    Parent ! {invalid_view, node(), Attempt},
                    erlang:error(injected_view_failure);
                false ->
                    meck:passthrough([])
            end
        end,
        ok = rpc:call(N2, meck, expect, [ekka_mnesia, cluster_view, View]),
        Test(Nodes, N2)
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

report_split([N1, N2] = Nodes) ->
    %% Drive just one heal cycle. Let any partition confirmation timers from join
    %% elapse before setting up the split.
    [ok = rpc:call(N, application, set_env, [ekka, cluster_autoheal, false]) || N <- Nodes],
    ok = timer:sleep(3100),
    Leader = rpc:call(N1, ekka_membership, leader, []),
    true = rpc:cast(N1, net_kernel, disconnect, [N2]),
    ok = wait_until(fun() -> split_observed(Nodes) end, 100),
    ?assertEqual(pong, rpc:call(N1, net_adm, ping, [N2])),
    ok = ekka_node_monitor:cast(Leader, {report_partition, N2}).

monitors(Nodes) ->
    [rpc:call(N, erlang, whereis, [ekka_node_monitor]) || N <- Nodes].

wait_for_invalid_view(Node, Attempt) ->
    receive {invalid_view, Node, Attempt} -> ok
    after 15000 -> error({no_invalid_view, Attempt})
    end.

%% A heal whose membership change already started reports an error; it must not be
%% replayed (that would stop the peer again).
t_autoheal_does_not_replay_started_heal(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [ah_started1, ah_started2]),
    try
        ok = rpc:call(N2, ekka, join, [N1]),
        [ok = install_test_guard(N, first_started_then_error) || N <- Nodes],
        true = rpc:cast(N1, net_kernel, disconnect, [N2]),
        ?assertEqual(ok, wait_until(fun() -> split_observed(Nodes) end, 100)),
        ?assertEqual(ok, wait_until(fun() -> joined(Nodes) end, 400)),
        %% The heal admission ran the change, so the retry classifier must not replay it;
        %% the reported error also prevents the application start.
        ok = timer:sleep(3000),
        ?assertEqual([0, 1], lists:sort([guard_calls(N) || N <- Nodes]))
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

%% A permanent refusal of the heal admission is reported without retrying.
t_autoheal_does_not_retry_permanent_refusal(_Config) ->
    [N1, N2] = Nodes = lists:map(fun start_slave_node/1, [ah_perm1, ah_perm2]),
    try
        ok = rpc:call(N2, ekka, join, [N1]),
        [ok = install_test_guard(N, always_rejected_permanent) || N <- Nodes],
        report_split(Nodes),
        ?assertEqual(ok, wait_until(fun() -> lists:max([guard_calls(N) || N <- Nodes]) >= 1 end, 400)),
        %% Wait longer than the autoheal retry delay: a transient refusal would be retried
        %% by now, a permanent one is not.
        ok = timer:sleep(2200),
        ?assertEqual([0, 1], lists:sort([guard_calls(N) || N <- Nodes])),
        ?assertNotEqual(Nodes, rpc:call(N1, ekka_mnesia, running_nodes, []))
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

t_autoheal_excludes_successful_target_on_retry(_Config) ->
    check_retry_excludes_target([ah_success1, ah_success2, ah_success3], fun() -> ok end).

t_autoheal_excludes_unknown_target_on_retry(_Config) ->
    check_retry_excludes_target([ah_unknown1, ah_unknown2, ah_unknown3],
                                fun() -> exit(unknown_heal_outcome) end).

check_retry_excludes_target(Names, Result) ->
    with_retry_split(Names, fun([Coordinator, Finished, Refused]) ->
        mock_heal(Finished, self(), fun(_) -> Result() end),
        mock_heal(Refused, self(), fun
            (1) -> {error, {guard_refused, busy}};
            (2) -> ok
        end),
        ok = ekka_node_monitor:cast(Coordinator, {heal_cluster, [Finished, Refused], []}),
        expect_heals([Finished, Refused, Refused])
    end).

t_autoheal_retry_handoff_keeps_exclusions_and_budget(_Config) ->
    with_retry_split([ah_handoff1, ah_handoff2, ah_handoff3], fun([Keeper, Coordinator, Finished]) ->
        Test = self(),
        ok = rpc:call(Keeper, meck, expect, [ekka_mnesia, cluster_nodes, fun(Kind) ->
            {current_stacktrace, Stack} = process_info(self(), current_stacktrace),
            case Kind =:= all andalso lists:keymember(ekka_autoheal, 1, Stack) of
                true -> Test ! {retry_view, node()};
                false -> ok
            end,
            meck:passthrough([Kind])
        end]),
        mock_heal(Keeper, Test, fun(_) -> {error, {guard_refused, busy}} end),
        mock_heal(Finished, Test, fun(_) -> ok end),
        mock_heal(Coordinator, Test, fun(_) -> {error, {guard_refused, busy}} end),
        ok = ekka_node_monitor:cast(Coordinator, {heal_cluster, [Keeper, Finished], []}),
        expect_heals([Keeper, Finished, Coordinator, Coordinator]),
        receive {retry_view, Keeper} -> ok after 5000 -> error(no_handoff_retry_view) end
    end).

t_autoheal_retries_peers_when_keeper_lacks_handoff(_Config) ->
    with_retry_split([ah_legacy1, ah_legacy2, ah_legacy3, ah_legacy4],
                     fun([Keeper, Coordinator, Finished, Refused]) ->
        Test = self(),
        ok = rpc:call(Keeper, meck, new, [ekka_autoheal, [passthrough, no_link]]),
        ok = rpc:call(Keeper, meck, expect, [ekka_autoheal, continue_heal, fun(Nodes, Excluded, Delay, Budget) ->
            Test ! {handoff_unsupported, node()},
            erlang:raise(error, undef, [{ekka_autoheal, continue_heal, [Nodes, Excluded, Delay, Budget], []}])
        end]),
        mock_heal(Coordinator, Test, fun(_) -> ok end),
        mock_heal(Finished, Test, fun(_) -> ok end),
        mock_heal(Refused, Test, fun
            (1) -> {error, {guard_refused, busy}};
            (2) -> ok
        end),
        ok = ekka_node_monitor:cast(Coordinator, {heal_cluster, [Finished, Refused], []}),
        expect_heals([Finished, Refused, Refused]),
        receive {handoff_unsupported, Keeper} -> ok after 5000 -> error(no_legacy_handoff) end
    end).

with_retry_split(Names, Check) ->
    [Seed | Peers] = Nodes = lists:map(fun start_slave_node/1, Names),
    try
        [ok = rpc:call(N, ekka, join, [Seed]) || N <- Peers],
        lists:foreach(fun(N) ->
            View = fun() -> {[node()], Nodes -- [node()]} end,
            ok = rpc:call(N, meck, new, [ekka_mnesia, [passthrough, no_link]]),
            ok = rpc:call(N, meck, expect, [ekka_mnesia, cluster_view, View])
        end, Nodes),
        Check(Nodes)
    after
        lists:foreach(fun ekka_ct:stop_slave/1, Nodes),
        snabbkaffe:stop()
    end.

mock_heal(Node, Test, Result) ->
    ok = rpc:call(Node, meck, new, [ekka_cluster, [passthrough, no_link]]),
    ok = rpc:call(Node, meck, expect, [ekka_cluster, heal, fun(shutdown_and_reboot) ->
        Key = {?MODULE, heal_attempts},
        Attempt = application:get_env(ekka, Key, 0) + 1,
        ok = application:set_env(ekka, Key, Attempt),
        Test ! {heal_called, node()},
        Result(Attempt)
    end]).

expect_heals([Node | Rest]) ->
    receive
        {heal_called, Target} -> ?assertEqual(Node, Target), expect_heals(Rest)
    after 10000 -> error({heal_not_dispatched, Node})
    end;
expect_heals([]) ->
    receive {heal_called, Node} -> error({unexpected_heal, Node})
    after 2500 -> ok
    end.

install_test_guard(Node, Mode) ->
    Key = {?MODULE, guard_calls},
    ok = rpc:call(Node, application, set_env, [ekka, Key, 0]),
    Guard = fun(_Node, _Targets, Action) ->
        N = application:get_env(ekka, Key, 0) + 1,
        ok = application:set_env(ekka, Key, N),
        case {Mode, N} of
            {first_rejected, 1} -> {error, busy};
            {always_rejected, _} -> {error, busy};
            {always_rejected_permanent, _} -> {error, not_allowed};
            {first_started_then_error, 1} -> _ = Action(), {error, started};
            _ -> Action()
        end
    end,
    ok = rpc:call(Node, ekka, callback, [membership_guard, Guard]).

guard_calls(Node) ->
    rpc:call(Node, application, get_env, [ekka, {?MODULE, guard_calls}, 0]).

split_observed(Nodes) ->
    lists:all(fun(N) -> rpc:call(N, ekka_mnesia, running_nodes, []) =:= [N] end, Nodes).

joined(Nodes) ->
    Sorted = lists:sort(Nodes),
    lists:all(fun(N) -> lists:sort(rpc:call(N, ekka_mnesia, running_nodes, [])) =:= Sorted end, Nodes).
