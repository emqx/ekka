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
        ]).

-ifdef(TEST).
%% Exported for test-suite access only. Not part of the public API.
-export([heal_node/1]).
-endif.

-record(autoheal, {delay, role, proc, timer}).

-type autoheal() :: #autoheal{}.

-export_type([autoheal/0]).

-define(DEFAULT_DELAY, 15000).
-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Autoheal): " ++ Format, Args)).

%% A heal request is re-confirmed (membership view re-read, heal plan recomputed) at most
%% this many times when the membership guard rejects it.
-define(HEAL_ATTEMPTS, 3).
-define(LEGACY_SHUTDOWN_TIMEOUT, 30000).
-define(LEGACY_SHUTDOWN_CONFIRM_TIMEOUT, 30000).

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

handle_msg({report_partition, _Node}, Autoheal = #autoheal{proc = Proc})
    when Proc =/= undefined ->
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

handle_msg(Msg = {create_splitview, Node}, Autoheal = #autoheal{delay = Delay, timer = TRef})
  when Node =:= node() ->
    ensure_cancel_timer(TRef),
    case ekka_membership:is_all_alive() of
        true ->
            case read_split_view() of
                {ok, SplitView} ->
                    HealPlan = find_heal_plan(SplitView),
                    case HealPlan of
                        {Candidates = [_ | _], Minority} ->
                            %% Non-empty list of candidates, choose a coordinator.
                            CoordNode = pick_coordinator(Candidates),
                            ekka_node_monitor:cast(CoordNode, {heal_cluster, Minority, SplitView});
                        {[], Cluster} ->
                            %% It's very unlikely but possible to have empty list of candidates.
                            ekka_node_monitor:cast(node(), {heal_cluster, Cluster, SplitView});
                        {} ->
                            ignore
                    end,
                    Autoheal#autoheal{timer = undefined};
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

handle_msg({heal_partition, SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(info, "Ignoring a heal request while another heal is in progress: ~p", [SplitView]),
    Autoheal;

handle_msg({'EXIT', Pid, normal}, Autoheal = #autoheal{proc = Pid}) ->
    Autoheal#autoheal{proc = undefined};
handle_msg({'EXIT', Pid, Reason}, Autoheal = #autoheal{proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~p", [Reason]),
    _Retry = ekka_node_monitor:run_after(1000, confirm_partition),
    Autoheal#autoheal{proc = undefined};

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
    case [{Node, Result} || Node <- Nodes, Result <- [heal_node(Node)], retryable(Result)] of
        [] ->
            ok;
        Failed ->
            retry_heal(Failed, Delay, Attempts - 1)
    end.

retry_heal(Failed, _Delay, 0) ->
    ?LOG(error, "Autoheal stopped retrying, nodes still not restarted: ~p. "
                "Inspect the logs on the affected nodes. A new partition event re-triggers "
                "autoheal.", [Failed]);
retry_heal(Failed, Delay, Attempts) ->
    timer:sleep(Delay),
    case read_split_view() of
        {ok, SplitView} ->
            case find_heal_plan(SplitView) of
                {_, Minority} ->
                    %% Only retry targets that were explicitly refused before stopping.
                    Rejected = [N || {N, _} <- Failed, lists:member(N, Minority)],
                    heal_nodes(Rejected, Delay, Attempts);
                {} ->
                    ok
            end;
        {error, Reason} ->
            ?LOG(warning, "Could not re-read the cluster view to retry autoheal: ~p", [Reason]),
            retry_heal(Failed, Delay, Attempts - 1)
    end.

%% Only a refusal that happened before the membership change started and is known to be
%% transient can be retried. Any other error means the change may have started or its
%% outcome is unknown (a badrpc, a failure after the stop/start, a failed legacy two-step
%% heal); replaying it could stop the peer again, so it is never retried.
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

heal_node(Node) ->
    Result = rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 30000),
    case Result of
        ok ->
            ?LOG(info, "Healed ~s for autoheal", [Node]),
            ok;
        {badrpc, {'EXIT', {function_clause,
                           [{ekka_cluster, heal, [shutdown_and_reboot], _} | _]}}} ->
            %% Old ekka on peer - fall back to legacy two-rpc path
            ?LOG(warning, "Heal ~s: peer runs legacy ekka; using the two-step fallback", [Node]),
            case legacy_heal(Node) of
                {error, {guard_refused, Reason}} -> {error, {legacy_heal_refused, Reason}};
                LegacyResult -> LegacyResult
            end;
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

%% Old peers have no membership guard, so a legacy failure is never a retryable refusal.
%% Once the shutdown step succeeded, replaying this path must not stop the peer again.
legacy_heal(Node) ->
    case rpc:call(Node, ekka_cluster, heal, [shutdown], ?LEGACY_SHUTDOWN_TIMEOUT) of
        ok ->
            legacy_reboot(Node);
        {badrpc, timeout} = Timeout ->
            %% The remote shutdown can finish after the RPC times out.
            ?LOG(warning, "Shutdown of ~s for autoheal timed out; waiting for Mnesia to stop",
                 [Node]),
            Deadline = erlang:monotonic_time(millisecond) + ?LEGACY_SHUTDOWN_CONFIRM_TIMEOUT,
            case confirm_legacy_shutdown(Node, Deadline) of
                ok -> legacy_reboot(Node);
                timeout ->
                    ?LOG(warning, "Could not confirm shutdown of ~s for autoheal", [Node]),
                    Timeout
            end;
        Error ->
            ?LOG(warning, "Shutdown of ~s for autoheal (legacy) failed: ~p", [Node, Error]),
            Error
    end.

confirm_legacy_shutdown(Node, Deadline) ->
    case Deadline - erlang:monotonic_time(millisecond) of
        Remaining when Remaining > 0 ->
            case rpc:call(Node, mnesia, system_info, [is_running], min(1000, Remaining)) of
                no -> ok;
                _ ->
                    timer:sleep(min(250, max(0, Deadline - erlang:monotonic_time(millisecond)))),
                    confirm_legacy_shutdown(Node, Deadline)
            end;
        _ ->
            timeout
    end.

legacy_reboot(Node) ->
    timer:sleep(rand:uniform(1000) + 100),
    case rpc:call(Node, ekka_cluster, heal, [reboot], 30000) of
        ok ->
            ?LOG(info, "Healed ~s for autoheal (legacy two-step)", [Node]),
            ok;
        Error ->
            ?LOG(warning, "Reboot of ~s for autoheal (legacy) failed: ~p", [Node, Error]),
            Error
    end.

ensure_cancel_timer(undefined) ->
    ok;
ensure_cancel_timer(TRef) ->
    catch erlang:cancel_timer(TRef).
