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

-record(autoheal, {delay, role, proc, timer}).

-type autoheal() :: #autoheal{}.

-export_type([autoheal/0]).

-define(DEFAULT_DELAY, 15000).
-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Autoheal): " ++ Format, Args)).

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
            Nodes = ekka_mnesia:cluster_nodes(all),
            case rpc:multicall(Nodes, ekka_mnesia, cluster_view, [], 30000) of
                {Views, []} ->
                    SplitView = find_split_view(Nodes, Views),
                    HealPlan = find_heal_plan(SplitView),
                    case HealPlan of
                        {Candidates = [_ | _], Minority} ->
                            %% Non-empty list of candidates, choose a coordinator.
                            CoordNode = ekka_membership:coordinator(Candidates),
                            ekka_node_monitor:cast(CoordNode, {heal_cluster, Minority, SplitView});
                        {[], Cluster} ->
                            %% It's very unlikely but possible to have empty list of candidates.
                            ekka_node_monitor:cast(node(), {heal_cluster, Cluster, SplitView});
                        {} ->
                            ignore
                    end,
                    Autoheal#autoheal{timer = undefined};
                {_Views, BadNodes} ->
                    ?LOG(critical, "Bad nodes found when autoheal: ~p", [BadNodes]),
                    Autoheal#autoheal{timer = ekka_node_monitor:run_after(Delay, {autoheal, Msg})}
            end;
        false ->
            Autoheal#autoheal{timer = ekka_node_monitor:run_after(Delay, {autoheal, Msg})}
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg({heal_partition, SplitView}, Autoheal = #autoheal{proc = undefined}) ->
    %% NOTE: Backward compatibility.
    case SplitView of
        %% No partitions.
        [] -> Autoheal;
        [{_, []}] -> Autoheal;
        %% Partitions.
        SplitView ->
            Proc = spawn_link(fun() -> heal_partition(SplitView) end),
            Autoheal#autoheal{role = coordinator, proc = Proc}
    end;

handle_msg({heal_cluster, Minority, SplitView}, Autoheal = #autoheal{proc = undefined}) ->
    Proc = spawn_link(fun() ->
        ?tp(notice, "Healing cluster partition", #{
            need_reboot => Minority,
            split_view => SplitView
        }),
        reboot_minority(Minority -- [node()])
    end),
    Autoheal#autoheal{role = coordinator, proc = Proc};

handle_msg({heal_partition, SplitView}, Autoheal = #autoheal{proc = _Proc}) ->
    ?LOG(critical, "Unexpected heal_partition msg: ~p", [SplitView]),
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
    MajorityView = lists:usort(fun compare_node_views/2, ClusterView),
    find_split_view(MajorityView).

compare_node_views({_N1, Running1, _}, {_N2, Running2, _}) ->
    Len1 = length(Running1),
    Len2 = length(Running2),
    case Len1 of
        %% Prefer partitions with higher number of surviving nodes.
        L when L > Len2 -> true;
        %% If number of nodes is the same, prefer those where current node is a survivor.
        %% Otherwise, sort by list of running nodes. If lists happen to be the same, this
        %% view will be excluded by usort.
        Len2 -> lists:member(node(), Running1) orelse Running1 < Running2;
        L when L < Len2 -> false
    end.

find_split_view([{_Node, _Running, []} | Views]) ->
    %% Node observes no partitions, ignore.
    find_split_view(Views);
find_split_view([View = {_Node, _Running, Partitioned} | Views]) ->
    %% Node observes some nodes as partitioned from it.
    %% These nodes need to be rebooted, and as such they should not be part of split view.
    Rest = lists:foldl(fun(N, Acc) -> lists:keydelete(N, 1, Acc) end, Views, Partitioned),
    [View | find_split_view(Rest)];
find_split_view([]) ->
    [].

find_heal_plan([{_Node, R0, P0} | Rest]) ->
    %% If we have more than one parition in split view, we need to reboot _all_ of the nodes
    %% in each view's partition (i.e. ⋃(Partitions)) for better safety. But then we need to
    %% find candidates to do it, as ⋃(Survivors) ∩ ⋃(Partitions).
    lists:foldl(
        fun({_, R, P}, {RAcc, PAcc}) ->
            {lists:usort((R -- PAcc) ++ (RAcc -- P)), lists:usort(P ++ PAcc)}
        end,
        {R0, P0},
        Rest
    );
find_heal_plan([]) ->
    {}.

heal_partition([{Nodes, []} | _] = SplitView) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    reboot_minority(Nodes -- [node()]);
heal_partition([{Majority, Minority}, {Minority, Majority}] = SplitView) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    reboot_minority(Minority).

reboot_minority(Minority) ->
    lists:foreach(fun shutdown/1, Minority),
    timer:sleep(rand:uniform(1000) + 100),
    lists:foreach(fun reboot/1, Minority),
    Minority.

shutdown(Node) ->
    Ret = rpc:call(Node, ekka_cluster, heal, [shutdown]),
    ?LOG(critical, "Shutdown ~s for autoheal: ~p", [Node, Ret]).

reboot(Node) ->
    Ret = rpc:call(Node, ekka_cluster, heal, [reboot]),
    ?LOG(critical, "Reboot ~s for autoheal: ~p", [Node, Ret]).

ensure_cancel_timer(undefined) ->
    ok;
ensure_cancel_timer(TRef) ->
    catch erlang:cancel_timer(TRef).
