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
        ]).

-ifdef(TEST).
%% Exported for test-suite access only. Not part of the public API.
-export([heal_node/1]).
-endif.

-record(autoheal, {delay, role, proc, timer}).

-define(PENDING_KEY, {?MODULE, pending}).

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
        {true, Delay} -> #autoheal{delay = Delay};
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

handle_msg({report_partition, Node}, Autoheal = #autoheal{proc = Proc})
    when Proc =/= undefined ->
    retain_report(Node),
    Autoheal;

handle_msg({report_partition, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef}) ->
    case ekka_membership:leader() =:= node() of
        true ->
            ensure_cancel_timer(TRef),
            TRef1 = ekka_node_monitor:run_after(Delay, {autoheal, {create_splitview, node()}}),
            Autoheal#autoheal{role = leader, timer = TRef1};
        false ->
            ?LOG(critical, "I am not leader, but received partition report from ~s", [Node]),
            Autoheal
    end;

handle_msg({create_splitview, Node}, Autoheal = #autoheal{delay = Delay, proc = Proc, timer = TRef})
  when Node =:= node(), Proc =/= undefined ->
    ensure_cancel_timer(TRef),
    ?LOG(info, "Deferring a heal planner run until the heal in progress finishes", []),
    Autoheal#autoheal{timer = ekka_node_monitor:run_after(Delay, {autoheal, {create_splitview, node()}})};

handle_msg(Msg = {create_splitview, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef})
  when Node =:= node() ->
    ensure_cancel_timer(TRef),
    case ekka_membership:is_all_alive() of
        true ->
            case read_split_view() of
                {ok, SplitView} ->
                    PendingReports = take_pending(),
                    Autoheal1 = plan_heal(SplitView, PendingReports, Delay, Autoheal),
                    Autoheal1#autoheal{timer = undefined};
                {error, Reason} ->
                    ?LOG(warning, "Could not read the cluster view for autoheal: ~p", [Reason]),
                    Autoheal#autoheal{timer = ekka_node_monitor:run_after(Delay, {autoheal, Msg})}
            end;
        false ->
            Autoheal#autoheal{timer = ekka_node_monitor:run_after(Delay, {autoheal, Msg})}
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg({heal_partition, SplitView}, Autoheal = #autoheal{proc = undefined, delay = Delay}) ->
    %% NOTE: Backward compatibility.
    case SplitView of
        %% No partitions.
        [] -> Autoheal;
        [{_, []}] -> Autoheal;
        %% Partitions.
        SplitView ->
            Proc = spawn_link(fun() -> heal_partition(SplitView, Delay) end),
            Autoheal#autoheal{role = coordinator, proc = Proc}
    end;

handle_msg({heal_cluster, Minority, SplitView},
           Autoheal = #autoheal{proc = undefined, delay = Delay}) ->
    Proc = spawn_link(fun() -> heal_cluster(Minority, SplitView, Delay) end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

handle_msg({heal_cluster, _Minority, _SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress", []),
    Autoheal;

handle_msg({continue_heal, Nodes, Excluded, Delay, Attempts},
           Autoheal = #autoheal{proc = undefined}) ->
    Proc = spawn_link(fun() -> heal_nodes(Nodes, Excluded, Delay, Attempts) end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

handle_msg({continue_heal, _, _, _, _}, Autoheal) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress", []),
    Autoheal;

handle_msg({heal_partition, SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress: ~p", [SplitView]),
    Autoheal;

handle_msg({'EXIT', Pid, normal}, Autoheal = #autoheal{proc = Pid}) ->
    Autoheal1 = Autoheal#autoheal{proc = undefined},
    resume_after_heal(Autoheal1);
handle_msg({'EXIT', Pid, Reason}, Autoheal = #autoheal{proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~p", [Reason]),
    case take_pending() of
        [] ->
            ok;
        Dropped ->
            ?LOG(warning, "Dropping retained partition report(s) after the crashed heal: ~p",
                 [Dropped])
    end,
    _ = ensure_cancel_timer(Autoheal#autoheal.timer),
    _Retry = ekka_node_monitor:run_after(1000, confirm_partition),
    Autoheal#autoheal{proc = undefined, timer = undefined};

handle_msg(Msg, Autoheal) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg, Autoheal]),
    Autoheal.

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
                    _ = ensure_cancel_timer(Autoheal#autoheal.timer),
                    TRef = ekka_node_monitor:run_after(0, {autoheal, {create_splitview, node()}}),
                    Autoheal#autoheal{timer = TRef};
                Leader ->
                    ?LOG(warning, "No longer the leader; forwarding retained report(s) to ~s: ~p",
                         [Leader, Pending]),
                    forward_reports(Leader, take_pending()),
                    Autoheal
            end
    end.

forward_reports(Leader, Reports) ->
    lists:foreach(fun(Node) -> ekka_node_monitor:cast(Leader, {report_partition, Node}) end,
                  Reports).

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
            case reported_heal_coordinator(Targets) of
                undefined ->
                    heal_all_reported_nodes(Targets, Delay, Autoheal);
                Coordinator ->
                    start_reported_heal(Coordinator, Targets, Delay, Autoheal)
            end
    end.

%% The coordinator must stay outside the reported set: heal_nodes/4 excludes the local
%% node from its targets, but a reporter can be the current leader/coordinator.
reported_heal_coordinator(Reports) ->
    Running = ekka_mnesia:running_nodes(),
    case [Node || Node <- Running -- Reports, ekka_membership:is_member(Node)] of
        [] -> undefined;
        Nodes -> ekka_membership:coordinator(Nodes)
    end.

%% Every running node reported, so no coordinator is left outside the report set. Heal
%% the other reporters first and let the last healed peer restart the runtime coordinator:
%% restarting this node stops ekka and would abort a local worker mid-heal.
heal_all_reported_nodes(Reports, Delay, Autoheal) ->
    Coordinator = ekka_membership:coordinator(Reports),
    Ordered = lists:delete(Coordinator, Reports) ++ [Coordinator],
    case Coordinator =:= node() of
        true ->
            ok;
        false ->
            ?LOG(warning, "Runtime coordinator is ~s, not this node; healing this node last anyway",
                 [Coordinator])
    end,
    ?LOG(info, "Every running node reported a partition; healing ~p and handing this node's "
                "restart to a healed peer", [Ordered]),
    start_handoff_heal_worker(Ordered, Delay, Autoheal).

start_reported_heal(Coordinator, Reports, Delay, Autoheal) when Coordinator =:= node() ->
    ?LOG(info, "Healing reported partition nodes serially: ~p", [Reports]),
    start_heal_worker(Reports, Delay, Autoheal);
start_reported_heal(Coordinator, Reports, Delay, Autoheal) ->
    ?LOG(info, "Delegating the reported-node heal to ~s: ~p", [Coordinator, Reports]),
    case rpc:call(Coordinator, ?MODULE, continue_heal, [Reports, [], Delay, ?HEAL_ATTEMPTS], 5000) of
        ok ->
            Autoheal;
        Error ->
            ?LOG(warning, "Could not hand off the reported-node heal to ~s: ~p",
                 [Coordinator, Error]),
            Autoheal
    end.

start_heal_worker([], _Delay, Autoheal) ->
    Autoheal;
start_heal_worker(Reports, _Delay, Autoheal = #autoheal{proc = Proc}) when Proc =/= undefined ->
    ?LOG(info, "Ignoring a reported-node heal while a heal is in progress: ~p", [Reports]),
    Autoheal;
start_heal_worker(Reports, Delay, Autoheal) ->
    Proc = spawn_link(fun() -> heal_nodes(Reports, [], Delay, ?HEAL_ATTEMPTS) end),
    Autoheal#autoheal{role = coordinator, proc = Proc}.

start_handoff_heal_worker(Ordered, Delay, Autoheal) ->
    Peers = Ordered -- [node()],
    case Peers of
        [] ->
            ?LOG(error, "No peer is available to take over this node's restart", []),
            Autoheal;
        _ ->
            start_handoff_heal_worker(Ordered, Peers, Delay, Autoheal)
    end.

start_handoff_heal_worker(_Ordered, _Peers, _Delay, Autoheal = #autoheal{proc = Proc})
  when Proc =/= undefined ->
    ?LOG(info, "Ignoring an all-reporters heal while a heal is in progress", []),
    Autoheal;
start_handoff_heal_worker(_Ordered, Peers, Delay, Autoheal) ->
    Proc = spawn_link(fun() -> heal_peers_then_handoff(Peers, Delay) end),
    Autoheal#autoheal{role = coordinator, proc = Proc}.

heal_peers_then_handoff(Peers, Delay) ->
    ok = heal_nodes(Peers, [], Delay, ?HEAL_ATTEMPTS),
    Peer = lists:last(Peers),
    case peers_recovered(Peers) of
        true ->
            ?LOG(info, "Handing this node's restart to ~s after healing ~p", [Peer, Peers]),
            case rpc:call(Peer, ?MODULE, continue_heal,
                          [[node()], Peers, Delay, ?HEAL_ATTEMPTS], 5000) of
                ok ->
                    ?LOG(info, "~s accepted the handoff of this node's restart", [Peer]);
                Error ->
                    ?LOG(critical, "~s did not accept the handoff of this node's restart: ~p. "
                                   "This node was not restarted; a new partition event is "
                                   "needed to retry.", [Peer, Error])
            end;
        false ->
            ?LOG(critical, "Not handing off this node's restart: not every healed peer is "
                           "running again: ~p. A new partition event is needed to retry.",
                 [Peers])
    end.

peers_recovered(Peers) ->
    lists:all(fun(Node) -> ekka_node:is_running(Node, ekka, 5000) end, Peers).

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
    Results = [{Node, heal_node(Node)} || Node <- Nodes],
    case lists:partition(fun({_, Result}) -> retryable(Result) end, Results) of
        {[], _} -> ok;
        {Failed, Settled} ->
            DoNotReplay = [Node || {Node, _} <- Settled] ++ Excluded,
            retry_heal({Failed, DoNotReplay}, Delay, Attempts - 1)
    end.

retry_heal({Failed, _Excluded}, _Delay, 0) ->
    ?LOG(error, "Autoheal stopped retrying, nodes still not restarted: ~p. "
                "Inspect the logs on the affected nodes. A new partition event re-triggers "
                "autoheal.", [Failed]);
retry_heal(State = {_Failed, Excluded}, Delay, Attempts) ->
    timer:sleep(Delay),
    case read_split_view() of
        {ok, SplitView} ->
            case find_heal_plan(SplitView) of
                {Candidates, Minority} ->
                    dispatch_retry(Candidates, Minority, Excluded, Delay, Attempts);
                {} ->
                    ok
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
            case rpc:call(Coordinator, ?MODULE, continue_heal,
                          [Targets, Excluded, Delay, Attempts], 5000) of
                ok ->
                    ok;
                {badrpc, {'EXIT', {undef, [{?MODULE, continue_heal, _, _} | _]}}} ->
                    %% An older coordinator cannot take over this retry.
                    heal_nodes(Targets -- [node()], Excluded, Delay, Attempts);
                Error ->
                    ?LOG(warning, "Could not hand off autoheal retry to ~s: ~p", [Coordinator, Error])
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

retain_report(Node) ->
    ?LOG(info, "Retained partition report from ~s while a heal is in progress", [Node]),
    erlang:put(?PENDING_KEY, lists:usort([Node | pending_reports()])).

pending_reports() ->
    case erlang:get(?PENDING_KEY) of
        undefined -> [];
        Pending -> Pending
    end.

take_pending() ->
    case erlang:erase(?PENDING_KEY) of
        undefined -> [];
        Pending -> Pending
    end.

heal_node(Node) ->
    Result = rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 30000),
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
            ?LOG(warning, "Heal ~s did not answer: ~p; assuming the node is restarting",
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
