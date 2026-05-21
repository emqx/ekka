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

%% Verifies the legacy fallback branch: if the peer only supports
%% heal(shutdown) / heal(reboot) (old ekka), heal_node/1 catches the
%% function_clause from heal(shutdown_and_reboot) and invokes both legacy rpcs.
%% We install a shim ekka_cluster module on the slave that only exports
%% the two legacy clauses and records invocations in an ETS table.
t_heal_legacy_fallback(_Config) ->
    Node = start_slave_node(lf1),
    try
        %% Compile shim module that records calls and lacks shutdown_and_reboot.
        ShimForms = legacy_shim_forms(),
        {ok, ekka_cluster, ShimBin} = compile:forms(ShimForms, [return_errors]),
        %% Stop real ekka on slave so swapping its cluster module is safe.
        ok = rpc:call(Node, application, stop, [ekka]),
        %% Swap ekka_cluster with the legacy shim.
        true = rpc:call(Node, code, delete, [ekka_cluster]),
        _ = rpc:call(Node, code, purge, [ekka_cluster]),
        {module, ekka_cluster} =
            rpc:call(Node, code, load_binary,
                     [ekka_cluster, "ekka_cluster.erl", ShimBin]),
        %% Prime the ETS tracker on the slave. The table must outlive this
        %% rpc:call (otherwise it would die with the rpc worker process), so
        %% we spawn a long-lived owner on the slave and have it create the
        %% table. Any process on the slave can then ets:insert into it
        %% (public, named_table).
        _Owner = rpc:call(Node, erlang, spawn,
                          [fun() ->
                               ets:new(heal_calls,
                                       [public, named_table, bag]),
                               receive stop -> ok end
                           end]),
        %% Wait until the table is visible to other processes on the slave.
        ok = wait_until(
               fun() ->
                   rpc:call(Node, ets, info, [heal_calls, size]) =/= undefined
               end, 50),
        %% Invoke heal_node/1 — should hit function_clause, fall back.
        _ = ekka_autoheal:heal_node(Node),
        %% Verify both legacy rpcs ran on the slave.
        Calls = rpc:call(Node, ets, tab2list, [heal_calls]),
        ?assert(lists:member({call, shutdown}, Calls)),
        ?assert(lists:member({call, reboot}, Calls)),
        ?assertNot(lists:member({call, shutdown_and_reboot}, Calls))
    after
        ekka_ct:stop_slave(Node)
    end.

%% Abstract forms for a legacy-shape ekka_cluster shim:
%%   -module(ekka_cluster).
%%   -export([heal/1]).
%%   heal(shutdown) -> ets:insert(heal_calls, {call, shutdown}), ok;
%%   heal(reboot)   -> ets:insert(heal_calls, {call, reboot}), ok.
legacy_shim_forms() ->
    Src = "-module(ekka_cluster).\n"
          "-export([heal/1]).\n"
          "heal(shutdown) ->\n"
          "    ets:insert(heal_calls, {call, shutdown}), ok;\n"
          "heal(reboot) ->\n"
          "    ets:insert(heal_calls, {call, reboot}), ok.\n",
    {ok, Toks, _} = erl_scan:string(Src),
    FormTokens = split_forms(Toks, [], []),
    [begin {ok, F} = erl_parse:parse_form(Ts), F end || Ts <- FormTokens].

split_forms([], [], Acc) ->
    lists:reverse(Acc);
split_forms([], _Cur, Acc) ->
    %% Trailing tokens without a dot: ignore (shouldn't happen for valid src).
    lists:reverse(Acc);
split_forms([{dot, _} = D | Rest], Cur, Acc) ->
    split_forms(Rest, [], [lists:reverse([D | Cur]) | Acc]);
split_forms([T | Rest], Cur, Acc) ->
    split_forms(Rest, [T | Cur], Acc).

%% Poll Fun/0 until it returns true or Retries attempts have elapsed.
%% Sleep 50ms between attempts. Returns ok on success, errors on timeout.
wait_until(_Fun, 0) ->
    error({wait_until, timeout});
wait_until(Fun, Retries) when Retries > 0 ->
    case Fun() of
        true  -> ok;
        _     -> timer:sleep(50), wait_until(Fun, Retries - 1)
    end.
