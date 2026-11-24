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

-module(ekka_autoheal).

-include_lib("snabbkaffe/include/trace.hrl").

-export([ init/0
        , enabled/0
        , proc/1
        , handle_msg/2
        , continue_heal/4
        , accept_retained_reports/1
        , heal_recovery_peer/2
        , heal_recovery_peer/3
        , observe_partition/2
        , accept_recovery_handoff/2
        , recovery_status/0
        , local_recovery_status/0
        , request_manual/3
        , authorize_manual/3
        , shadowed_owner/1
        ]).

-ifdef(TEST).
%% Exported for test-suite access only. Not part of the public API.
-export([heal_node/1]).
-endif.

-record(autoheal, {delay, role, proc, timer}).

-define(PENDING_KEY, {?MODULE, pending}).
-define(REPORT_SEQ, {?MODULE, report_sequence}).
-define(RELEASED_KEY, {?MODULE, keep_release_reports}).
-define(RECOVERY_KEY, {?MODULE, recovery_candidate}).
-define(WORKER_FAILED_KEY, {?MODULE, worker_failed}).
-define(WORKER_KEY, {?MODULE, worker}).
%% Older workers only subtract exclusions from node lists, so the marker is safe to forward.
-define(REPORTED_HEAL, {?MODULE, reported_heal}).

-type autoheal() :: #autoheal{}.

-export_type([autoheal/0]).

-define(DEFAULT_DELAY, 15000).
-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Autoheal): " ++ Format, Args)).

%% A heal request is re-confirmed (membership view re-read, heal plan recomputed) at most
%% this many times when the membership guard rejects it.
-define(HEAL_ATTEMPTS, 3).

init() ->
    case enabled() of
        {true, Delay} ->
            Autoheal = #autoheal{delay = Delay},
            case ekka_isolated_heal:restore_shadow() of
                true -> schedule_planner(Delay, Autoheal);
                false -> Autoheal
            end;
        false -> undefined
    end.

enabled() ->
    case ekka:env(cluster_autoheal, true) of
        false -> false;
        true  -> {true, ?DEFAULT_DELAY};
        Delay when is_integer(Delay) ->
            {true, Delay}
    end.

proc(undefined) -> undefined;
proc(#autoheal{proc = Proc}) -> Proc.

handle_msg(Msg, undefined) ->
    ?LOG(error, "Autoheal not enabled! Unexpected msg: ~p", [Msg]), undefined;

handle_msg({report_partition, Node}, Autoheal) ->
    case ekka_isolated_heal:active() of
        true ->
            retain_report(Node),
            Autoheal;
        false ->
            handle_report(Node, Autoheal)
    end;

handle_msg({retained_reports, Reports}, Autoheal = #autoheal{proc = undefined, delay = Delay}) ->
    retain_reports(Reports),
    case ekka_membership:leader() =:= node() of
        true ->
            schedule_planner(0, Autoheal);
        false ->
            %% Two nodes can disagree about leadership while their views converge.
            schedule_planner(max(1000, Delay), Autoheal)
    end;
handle_msg({retained_reports, Reports}, Autoheal) ->
    retain_reports(Reports),
    Autoheal;

handle_msg({create_splitview, Node}, Autoheal = #autoheal{timer = undefined})
  when Node =:= node() ->
    Autoheal;
handle_msg({create_splitview, Node}, Autoheal = #autoheal{delay = Delay, proc = Proc})
  when Node =:= node(), Proc =/= undefined ->
    ?LOG(info, "Deferring a heal planner run until the heal in progress finishes", []),
    schedule_planner(Delay, Autoheal);
handle_msg({create_splitview, Node}, Autoheal = #autoheal{delay = Delay})
  when Node =:= node() ->
    case ekka_isolated_heal:active() of
        true -> recovery_tick(Delay, Autoheal);
        false -> plan_split(Delay, Autoheal)
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg(Request, Autoheal)
  when element(1, Request) =:= heal_partition;
       element(1, Request) =:= heal_cluster;
       element(1, Request) =:= continue_heal ->
    case ekka_isolated_heal:active() of
        true ->
            ?LOG(warning, "Deferring ordinary autoheal during isolated recovery: ~p", [Request]),
            Autoheal;
        false ->
            start_heal_worker(Request, Autoheal)
    end;

handle_msg({recovery_candidate, Pid, Target, Round, Peer}, Autoheal = #autoheal{proc = Pid}) ->
    put(?RECOVERY_KEY, {Target, Round, Peer}),
    Autoheal;
handle_msg({recovery_finished, Pid}, Autoheal = #autoheal{proc = Pid}) ->
    erase(?RECOVERY_KEY),
    Autoheal;
handle_msg({recovery_decision, Operation, Round}, Autoheal) ->
    ok = ekka_isolated_heal:decide(Operation, Round, report_sequences()),
    Autoheal;
handle_msg({close_recovery_shadow, Round}, Autoheal) ->
    case ekka_isolated_heal:close_shadow(Round) of
        resume ->
            resume_after_heal(cancel_planner(Autoheal));
        keep ->
            Autoheal
    end;
handle_msg({recovery_result, Key, Operation, Result}, Autoheal) ->
    ok = ekka_isolated_heal:result(Key, Operation, Result),
    Autoheal;
handle_msg({worker_failed, Pid}, Autoheal = #autoheal{proc = Pid}) ->
    erlang:put(?WORKER_FAILED_KEY, true),
    Autoheal;
handle_msg({worker_failed, _Pid}, Autoheal) ->
    Autoheal;
handle_msg({'EXIT', Pid, normal}, Autoheal = #autoheal{proc = Pid}) ->
    Autoheal1 = Autoheal#autoheal{proc = undefined},
    Kind = take_worker(),
    case {erase(?RECOVERY_KEY), take_worker_failed()} of
        {{Target, Round, Peer}, _} ->
            ok = ekka_isolated_heal:start(Target, Round, Peer),
            schedule_planner(Autoheal#autoheal.delay, Autoheal1);
        {undefined, true} ->
            drop_pending("the heal did not complete"),
            cancel_planner(Autoheal1);
        {undefined, false} when Kind =:= handoff ->
            drop_pending("the coordinator restart was handed to a peer"),
            cancel_planner(Autoheal1);
        {undefined, false} ->
            resume_after_heal(Autoheal1)
    end;
handle_msg({'EXIT', Pid, Reason}, Autoheal = #autoheal{proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~p", [Reason]),
    _ = take_worker_failed(),
    _ = take_worker(),
    case erase(?RECOVERY_KEY) of
        {Target, Round, Peer} ->
            ok = ekka_isolated_heal:start(Target, Round, Peer),
            schedule_planner(Autoheal#autoheal.delay, Autoheal#autoheal{proc = undefined});
        undefined ->
            failed_worker(Autoheal)
    end;

handle_msg(Msg, Autoheal) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg, Autoheal]),
    Autoheal.

failed_worker(Autoheal) ->
    case take_pending() of
        [] ->
            ok;
        Dropped ->
            ?LOG(warning, "Dropping retained partition report(s) after the crashed heal: ~p",
                 [Dropped])
    end,
    _Retry = ekka_node_monitor:run_after(1000, confirm_partition),
    cancel_planner(Autoheal#autoheal{proc = undefined}).

start_heal_worker({heal_partition, SplitView}, Autoheal = #autoheal{proc = undefined, delay = Delay}) ->
    %% NOTE: Backward compatibility.
    case SplitView of
        %% No partitions.
        [] -> Autoheal;
        [{_, []}] -> Autoheal;
        %% Partitions.
        SplitView ->
            remember_worker(normal),
            Proc = spawn_link(fun() -> worker_result(fun() -> heal_partition(SplitView, Delay) end) end),
            Autoheal#autoheal{role = coordinator, proc = Proc}
    end;

start_heal_worker({heal_cluster, Minority, SplitView},
           Autoheal = #autoheal{proc = undefined, delay = Delay}) ->
    remember_worker(normal),
    Proc = spawn_link(fun() -> worker_result(fun() -> heal_cluster(Minority, SplitView, Delay) end) end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

start_heal_worker({heal_cluster, _Minority, _SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress", []),
    Autoheal;

start_heal_worker({continue_heal, Nodes, Excluded, Delay, Attempts},
           Autoheal = #autoheal{proc = undefined}) ->
    remember_worker(normal),
    Proc = spawn_link(fun() -> worker_result(fun() -> heal_nodes(Nodes, Excluded, Delay, Attempts) end) end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

start_heal_worker({continue_heal, _, _, _, _}, Autoheal) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress", []),
    Autoheal;

start_heal_worker({heal_partition, SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress: ~p", [SplitView]),
    Autoheal.

handle_report(Node, Autoheal = #autoheal{proc = Proc}) when Proc =/= undefined ->
    retain_report(Node),
    Autoheal;
handle_report(Node, Autoheal = #autoheal{delay = Delay}) ->
    case ekka_membership:leader() =:= node() of
        true ->
            case get(?RELEASED_KEY) of
                true -> retain_report(Node);
                _ -> ok
            end,
            schedule_planner(Delay, Autoheal#autoheal{role = leader});
        false ->
            ?LOG(critical, "I am not leader, but received partition report from ~s", [Node]),
            Autoheal
    end.

observe_partition(Event, Node) ->
    ekka_isolated_heal:event(Event, Node, report_sequences()).

request_manual(Target, Round, Instance) ->
    gen_server:call(ekka_node_monitor, {autoheal_manual_recovery, Target, Round, Instance}, 10000).

authorize_manual(Target, Round, Instance) ->
    ekka_isolated_heal:authorize_manual(Target, Round, Instance, report_sequences()).

recovery_status() ->
    gen_server:call(ekka_node_monitor, autoheal_recovery_status).

local_recovery_status() ->
    #{recovery => ekka_isolated_heal:status(), reports => report_sequences()}.

shadowed_owner(Owner) ->
    ekka_isolated_heal:shadowed_owner(Owner).

accept_recovery_handoff(State, Reports) ->
    ekka_isolated_heal:accept_handoff(State#{owner_reports => Reports}).

recovery_tick(Delay, Autoheal) ->
    case ekka_isolated_heal:tick(report_sequences(), Delay) of
        keep -> schedule_planner(Delay, Autoheal);
        {consume, Snapshot} ->
            consume_snapshot(Snapshot),
            schedule_planner(Delay, Autoheal);
        resume ->
            put(?RELEASED_KEY, true),
            resume_after_heal(cancel_planner(Autoheal))
    end.

plan_split(Delay, Autoheal) ->
    case ekka_membership:is_all_alive() of
        true ->
            case read_split_view() of
                {ok, SplitView} ->
                    erase(?RELEASED_KEY),
                    plan_after_view(SplitView, take_pending(), Delay, cancel_planner(Autoheal));
                {error, Reason} ->
                    ?LOG(warning, "Could not read the cluster view for autoheal: ~p", [Reason]),
                    schedule_planner(Delay, Autoheal)
            end;
        false ->
            schedule_planner(Delay, Autoheal)
    end.

find_split_view(Nodes, Views) ->
    ClusterView = lists:zipwith(
        fun(N, {Running, Stopped}) -> {N, Running, Stopped} end,
        Nodes,
        Views
    ),
    MajorityView = lists:sort(fun compare_node_views/2, ClusterView),
    find_split_view(MajorityView).

compare_node_views({_N1, Running1, _}, {_N2, Running2, _}) ->
    Len1 = length(Running1),
    Len2 = length(Running2),
    case Len1 of
        %% Prefer partitions with higher number of surviving nodes.
        L when L > Len2 -> true;
        %% If number of nodes is the same, sort by list of running nodes.
        Len2 -> Running1 < Running2;
        L when L < Len2 -> false
    end.

find_split_view([{_Node, _Running, []} | Views]) ->
    %% Node observes no partitions, ignore.
    find_split_view(Views);
find_split_view([{Node, Running, Partitioned} | Views]) ->
    %% Node observes some nodes as partitioned from it.
    %% These nodes need to be rebooted, and as such they should not be part of split view.
    PViews = [PV || PV = {PN, _, _} <- Views, lists:member(PN, Partitioned)],
    %% Taints are nodes that are connected to the partitioned nodes, that should also be rebooted
    %% to avoid overlapping partitions.
    Taints = lists:append([PRunning || {_, PRunning, _} <- PViews]),
    ViewTainted = {Node, Running -- Taints, lists:usort(Partitioned ++ Taints)},
    % io:format("ViewTainted: ~p~n", [ViewTainted]),
    [ViewTainted | find_split_view(Views -- PViews)];
find_split_view([]) ->
    [].

find_heal_plan([{_Node, R0, P0} | Rest]) ->
    %% If we have more than one parition in split view, we need to reboot _all_ of the nodes
    %% in each view's partition (i.e. ⋃(Partitions)) for better safety. But then we need to
    %% find candidates to do it, as ⋃(Running) ∖ ⋃(Partitions).
    {_Nodes, Rs, Ps} = lists:unzip3(Rest),
    URunning = ordsets:union(lists:map(fun ordsets:from_list/1, [R0 | Rs])),
    UPartitions = ordsets:union(lists:map(fun ordsets:from_list/1, [P0 | Ps])),
    {ordsets:subtract(URunning, UPartitions), UPartitions};
find_heal_plan([]) ->
    {}.

plan_after_view(SplitView, [], Delay, Autoheal) ->
    plan_heal(SplitView, [], Delay, Autoheal);
plan_after_view(SplitView, Reports, Delay, Autoheal) ->
    case ekka_membership:leader() of
        Leader when Leader =:= node() ->
            plan_heal(SplitView, Reports, Delay, Autoheal);
        Leader ->
            forward_reports(Leader, Reports),
            Autoheal
    end.

%% A complete membership view hides the route divergence that the retained reports are
%% about, so those reporters are restarted serially. The normal planner still wins when
%% the view shows a split: the reports were only needed to get here.
plan_heal(SplitView, PendingReports, Delay, Autoheal) ->
    case find_heal_plan(SplitView) of
        {Candidates = [_ | _], Minority} ->
            %% Non-empty list of candidates, choose a coordinator.
            CoordNode = pick_coordinator(Candidates),
            ekka_node_monitor:cast(CoordNode, {heal_cluster, Minority, SplitView}),
            Autoheal;
        {[], Cluster} ->
            %% It's very unlikely but possible to have empty list of candidates.
            ekka_node_monitor:cast(node(), {heal_cluster, Cluster, SplitView}),
            Autoheal;
        {} ->
            heal_reported_nodes(PendingReports, Delay, Autoheal)
    end.

resume_after_heal(Autoheal) ->
    case pending_reports() of
        [] ->
            Autoheal;
        Pending ->
            case ekka_membership:leader() of
                Leader when Leader =:= node() ->
                    ?LOG(info, "Heal worker exited; planning again for retained report(s): ~p",
                         [Pending]),
                    schedule_planner(0, Autoheal);
                Leader ->
                    forward_reports(Leader, take_pending()),
                    cancel_planner(Autoheal)
            end
    end.

forward_reports(Leader, Reports) ->
    ?LOG(info, "Forwarding retained partition reports ~p to the leader ~s", [Reports, Leader]),
    case rpc:call(Leader, ?MODULE, accept_retained_reports, [Reports], 5000) of
        ok ->
            ok;
        {badrpc, {'EXIT', {undef, [{?MODULE, accept_retained_reports, _, _} | _]}}} ->
            lists:foreach(fun(Node) ->
                ekka_node_monitor:cast(Leader, {report_partition, Node})
            end, Reports);
        Error ->
            ?LOG(warning, "Could not confirm delivery of retained reports to ~s: ~p", [Leader, Error])
    end.

accept_retained_reports(Reports) when is_list(Reports) ->
    ekka_node_monitor ! {autoheal, {retained_reports, Reports}},
    ok.

heal_reported_nodes([], _Delay, Autoheal) ->
    Autoheal;
heal_reported_nodes(Reports, Delay, Autoheal) ->
    Running = ekka_mnesia:running_nodes(),
    Targets = [Node || Node <- Reports, lists:member(Node, Running)],
    case Targets of
        [] ->
            ?LOG(info, "Retained reporters are no longer running; no heal is needed: ~p",
                 [Reports]),
            Autoheal;
        _ ->
            ?LOG(info, "Membership view is complete; autohealing retained reporters serially: ~p",
                 [Targets]),
            Candidates = [Node || Node <- Running -- Targets, ekka_membership:is_member(Node)],
            start_reported_heal(Candidates, Targets, Delay, Autoheal)
    end.

start_reported_heal([], Reports, Delay, Autoheal) ->
    Peers = Reports -- [node()],
    case Peers of
        [] ->
            ?LOG(error, "No peer is available to take over this node's restart", []),
            Autoheal;
        _ ->
            ?LOG(info, "Healing retained reporters ~p before handing off this node's restart",
                 [Peers]),
            remember_worker(handoff),
            Proc = spawn_link(fun() ->
                worker_result(fun() -> heal_peers_then_handoff(Peers, Delay) end)
            end),
            Autoheal#autoheal{role = coordinator, proc = Proc}
    end;
start_reported_heal(Candidates, Reports, Delay, Autoheal) ->
    Coordinator = pick_coordinator(Candidates),
    case Coordinator =:= node() of
        true ->
            handle_msg({continue_heal, Reports, [?REPORTED_HEAL], Delay, ?HEAL_ATTEMPTS}, Autoheal);
        false ->
            ?LOG(info, "Delegating the reported-node heal to ~s: ~p", [Coordinator, Reports]),
            case request_handoff(Coordinator, Reports, [?REPORTED_HEAL], Delay, ?HEAL_ATTEMPTS) of
                unavailable ->
                    start_reported_heal(Candidates -- [Coordinator], Reports, Delay, Autoheal);
                _ ->
                    Autoheal
            end
    end.

heal_peers_then_handoff(Peers, Delay) ->
    case heal_peers(Peers, Delay) of
        ok ->
            handoff_coordinator(lists:reverse(Peers), Peers, Delay);
        {error, Failure} = Error ->
            ?LOG(critical, "Cannot hand off this node's restart after a failed peer heal: ~p. "
                           "Waiting for a new partition event.", [Failure]),
            Error
    end.

heal_peers([], _Delay) ->
    ok;
heal_peers([Peer | Rest], Delay) ->
    case heal_peer(Peer, Delay, ?HEAL_ATTEMPTS) of
        ok -> heal_peers(Rest, Delay);
        Error -> {error, {Peer, Error}}
    end.

heal_peer(Peer, Delay, Attempts) ->
    Result = heal_node(Peer),
    case Attempts > 1 andalso retryable(Result) of
        true ->
            timer:sleep(Delay),
            heal_peer(Peer, Delay, Attempts - 1);
        false ->
            Result
    end.

handoff_coordinator([], _Peers, _Delay) ->
    ?LOG(critical, "No healed peer can take over this node's restart. "
                   "Waiting for a new partition event.", []),
    {error, no_handoff_peer};
handoff_coordinator([Peer | Rest], Peers, Delay) ->
    ?LOG(info, "Requesting the restart of this node on ~s after healing ~p", [Peer, Peers]),
    case request_handoff(Peer, [node()], [?REPORTED_HEAL | Peers], Delay, ?HEAL_ATTEMPTS) of
        ok ->
            ?LOG(info, "Handoff request for this node's restart was sent to ~s", [Peer]);
        unavailable ->
            handoff_coordinator(Rest, Peers, Delay);
        {error, _} = Error ->
            Error
    end.

request_handoff(Coordinator, Targets, Excluded, Delay, Attempts) ->
    case rpc:call(Coordinator, ?MODULE, continue_heal,
                  [Targets, Excluded, Delay, Attempts], 5000) of
        ok ->
            ok;
        {badrpc, {'EXIT', {undef, [{?MODULE, continue_heal, _, _} | _]}}} ->
            ?LOG(warning, "Node ~s does not support autoheal handoff", [Coordinator]),
            unavailable;
        Error ->
            ?LOG(critical, "Unconfirmed autoheal handoff of ~p to ~s: ~p. "
                           "Waiting for a new partition event.", [Targets, Coordinator, Error]),
            {error, Error}
    end.

pick_coordinator(Candidates) ->
    case lists:member(node(), Candidates) of
        true -> node();
        false -> ekka_membership:coordinator(Candidates)
    end.

heal_cluster(Minority, SplitView, Delay) ->
    ?tp(critical, "Healing cluster partition", #{
        need_reboot => Minority,
        split_view => SplitView
    }),
    heal_nodes(Minority -- [node()], Delay, ?HEAL_ATTEMPTS).

heal_partition([{Nodes, []} | _] = SplitView, Delay) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    heal_nodes(Nodes -- [node()], Delay, ?HEAL_ATTEMPTS);
heal_partition([{Majority, Minority}, {Minority, Majority}] = SplitView, Delay) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    heal_nodes(Minority, Delay, ?HEAL_ATTEMPTS).

heal_nodes(Nodes, Delay, Attempts) ->
    heal_nodes(Nodes, [], Delay, Attempts).

heal_nodes(Nodes, Excluded, Delay, Attempts) ->
    case heal_targets(Nodes, []) of
        {isolation_pending, Target, Result} ->
            {error, [{Target, Result}]};
        Results ->
            handle_heal_results(Results, Excluded, Delay, Attempts)
    end.

handle_heal_results(Results, Excluded, Delay, Attempts) ->
    case lists:partition(fun({_, Result}) -> retryable(Result) end, Results) of
        {[], Settled} ->
            case [{Node, Result} || {Node, Result} <- Settled, Result =/= ok] of
                [] -> ok;
                Failed -> {error, Failed}
            end;
        {Failed, Settled} ->
            DoNotReplay = [Node || {Node, _} <- Settled] ++ Excluded,
            retry_heal({Failed, DoNotReplay}, Delay, Attempts - 1)
    end.

heal_targets([], Results) ->
    lists:reverse(Results);
heal_targets([Target | Rest], Results) ->
    case heal_target(Target) of
        {isolation_pending, _, _} = Pending -> Pending;
        Result -> heal_targets(Rest, [{Target, Result} | Results])
    end.

heal_target(Node) ->
    case recovery_peer(Node) of
        {ok, Peer} ->
            Id = {node(), make_ref()},
            ok = notify_recovery(Node, Id, Peer),
            Result = heal_recovery_peer(Node, Id),
            case Result of
                {error, {heal_isolation, _}} ->
                    {isolation_pending, Node, Result};
                {error, {heal_interrupted, _}} ->
                    {isolation_pending, Node, Result};
                {badrpc, _} ->
                    {isolation_pending, Node, Result};
                _ ->
                    ekka_node_monitor ! {autoheal, {recovery_finished, self()}},
                    Result
            end;
        unavailable ->
            heal_node(Node)
    end.

recovery_peer(Target) ->
    case ekka_mnesia:cluster_nodes(all) -- [node(), Target] of
        [Peer] when Peer =/= Target ->
            case ekka_membership:leader() =:= node() andalso
                 ekka_heal_recovery:capability() =:= 1 of
                true ->
                    case rpc:multicall([Target, Peer], ekka_heal_recovery, capability, [], 1000) of
                        {[1, 1], []} -> {ok, Peer};
                        Capability ->
                            ?LOG(warning,
                                 "Autoheal recovery protocol unavailable for ~p and ~p: ~p",
                                 [Target, Peer, Capability]),
                            unavailable
                    end;
                false -> unavailable
            end;
        _ ->
            unavailable
    end.

notify_recovery(Target, Round, Peer) ->
    ekka_node_monitor ! {autoheal, {recovery_candidate, self(), Target, Round, Peer}},
    ok.

retry_heal({Failed, _Excluded}, _Delay, 0) ->
    ?LOG(error, "Autoheal stopped retrying, nodes still not restarted: ~p. "
                "Inspect the logs on the affected nodes. A new partition event re-triggers "
                "autoheal.", [Failed]),
    {error, Failed};
retry_heal(State = {Failed, Excluded}, Delay, Attempts) ->
    timer:sleep(Delay),
    case read_split_view() of
        {ok, SplitView} ->
            case find_heal_plan(SplitView) of
                {Candidates, Minority} ->
                    dispatch_retry(Candidates, Minority, Excluded, Delay, Attempts);
                {} ->
                    case lists:member(?REPORTED_HEAL, Excluded) of
                        true ->
                            heal_nodes([Node || {Node, _} <- Failed], Excluded, Delay, Attempts);
                        false ->
                            ok
                    end
            end;
        {error, Reason} ->
            ?LOG(warning, "Could not re-read the cluster view to retry autoheal: ~p", [Reason]),
            retry_heal(State, Delay, Attempts - 1)
    end.

dispatch_retry(Candidates, Minority, Excluded, Delay, Attempts) ->
    Targets = Minority -- Excluded,
    case {Targets, lists:member(node(), Minority), Candidates} of
        {[], _, _} ->
            ok;
        {_, true, [_ | _]} ->
            Coordinator = pick_coordinator(Candidates),
            case request_handoff(Coordinator, Targets, Excluded, Delay, Attempts) of
                unavailable ->
                    heal_nodes(Targets -- [node()], Excluded, Delay, Attempts);
                Result ->
                    Result
            end;
        _ ->
            heal_nodes(Targets -- [node()], Excluded, Delay, Attempts)
    end.

continue_heal(Targets, Excluded, Delay, Attempts) ->
    ekka_node_monitor ! {autoheal, {continue_heal, Targets, Excluded, Delay, Attempts}},
    ok.

%% Only a refusal that happened before the membership change started and is known to be
%% transient can be retried. Any other error means the change may have started or its
%% outcome is unknown (a badrpc or a failure after the stop/start). Replaying it
%% could stop the peer again.
retryable({error, {guard_refused, Reason}}) -> ekka_mnesia:transient_refusal(Reason);
retryable(_) -> false.

read_split_view() ->
    Nodes = ekka_mnesia:cluster_nodes(all),
    case rpc:multicall(Nodes, ekka_mnesia, cluster_view, [], 30000) of
        {Views, []} when length(Views) =:= length(Nodes) ->
            case [N || {N, View} <- lists:zip(Nodes, Views), not valid_view(N, View)] of
                [] -> {ok, find_split_view(Nodes, Views)};
                Invalid -> {error, {invalid_cluster_views, Invalid}}
            end;
        {_Views, BadNodes} ->
            {error, {cluster_view_unavailable, BadNodes}}
    end.

valid_view(Node, {Running, Stopped}) ->
    is_node_list(Running) andalso is_node_list(Stopped) andalso
        lists:member(Node, Running) andalso
        ordsets:is_disjoint(ordsets:from_list(Running), ordsets:from_list(Stopped));
valid_view(_, _) ->
    false.

is_node_list([Node | Rest]) when is_atom(Node) -> is_node_list(Rest);
is_node_list([]) -> true;
is_node_list(_) -> false.

remember_worker(Kind) ->
    _ = erlang:erase(?WORKER_FAILED_KEY),
    erlang:put(?WORKER_KEY, Kind).

take_worker() ->
    case erlang:erase(?WORKER_KEY) of
        {Kind, _Targets} -> Kind;
        Kind -> Kind
    end.

drop_pending(Reason) ->
    case take_pending() of
        [] ->
            ok;
        Dropped ->
            ?LOG(warning, "Dropping retained partition report(s) because ~s: ~p",
                 [Reason, Dropped])
    end.

schedule_planner(Delay, Autoheal) ->
    _ = ensure_cancel_timer(Autoheal#autoheal.timer),
    TRef = ekka_node_monitor:run_after(Delay, {autoheal, {create_splitview, node()}}),
    Autoheal#autoheal{timer = TRef}.

cancel_planner(Autoheal) ->
    _ = ensure_cancel_timer(Autoheal#autoheal.timer),
    Autoheal#autoheal{timer = undefined}.

worker_result(Fun) ->
    case Fun() of
        {error, _} -> ekka_node_monitor ! {autoheal, {worker_failed, self()}};
        _ -> ok
    end.

take_worker_failed() ->
    case erlang:erase(?WORKER_FAILED_KEY) of
        undefined -> false;
        Failed -> Failed
    end.

retain_report(Node) ->
    ?LOG(info, "Retained partition report from ~s", [Node]),
    retain_reports([Node]).

retain_reports(Reports) ->
    lists:foreach(fun retain_sequence/1, Reports).

retain_sequence(Node) ->
    Seq = case get(?REPORT_SEQ) of undefined -> 1; Prev -> Prev + 1 end,
    put(?REPORT_SEQ, Seq),
    put(?PENDING_KEY, (report_sequences())#{Node => Seq}),
    ekka_isolated_heal:report(Node).

report_sequences() ->
    case get(?PENDING_KEY) of
        undefined -> #{};
        Reports when is_map(Reports) -> Reports;
        Reports when is_list(Reports) -> maps:from_list([{Node, 0} || Node <- Reports])
    end.

pending_reports() -> lists:sort(maps:keys(report_sequences())).

take_pending() ->
    Reports = pending_reports(),
    erase(?PENDING_KEY),
    Reports.

consume_snapshot(Snapshot) ->
    Remaining = maps:filter(
        fun(Node, Seq) -> Seq > maps:get(Node, Snapshot, -1) end,
        report_sequences()
    ),
    put(?PENDING_KEY, Remaining),
    ?LOG(info, "Autoheal consumed report snapshot ~p; retained ~p", [Snapshot, Remaining]).

heal_node(Node) ->
    call_heal(Node, shutdown_and_reboot).

heal_recovery_peer(Node, Id) ->
    call_heal(Node, {shutdown_and_reboot, Id}).

heal_recovery_peer(Node, Id, Round) ->
    call_heal(Node, {shutdown_and_reboot, Id, Round}).

call_heal(Node, Request) ->
    Result = rpc:call(Node, ekka_cluster, heal, [Request], 30000),
    case Result of
        ok ->
            ?LOG(info, "Healed ~s for autoheal", [Node]),
            ok;
        {error, {guard_refused, Reason}} ->
            ?LOG(warning, "Heal ~s was refused: ~p", [Node, Reason]),
            {error, {guard_refused, Reason}};
        {error, Reason} ->
            ?LOG(warning, "Heal ~s failed: ~p", [Node, Reason]),
            {error, Reason};
        {badrpc, Reason} ->
            ?LOG(warning, "Heal ~s did not confirm completion: ~p",
                 [Node, Reason]),
            {badrpc, Reason};
        Other ->
            ?LOG(warning, "Heal ~s returned ~p", [Node, Other]),
            Other
    end.

ensure_cancel_timer(undefined) ->
    ok;
ensure_cancel_timer(TRef) ->
    catch erlang:cancel_timer(TRef).
