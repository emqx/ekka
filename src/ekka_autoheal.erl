%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_autoheal).

-export([init/0, enabled/0, proc/1, handle_msg/2]).

-record(?MODULE, {role, proc, timer}).

-define(DELAY, 12000).
-define(LOG(Level, Format, Args), logger:Level("Ekka(Autoheal): " ++ Format, Args)).

init() ->
    case enabled() of
        false -> undefined;
        true  -> #?MODULE{}
    end.

enabled() ->
    ekka:env(cluster_autoheal, false).

proc(undefined) ->
    undefined;
proc(#?MODULE{proc = Proc}) ->
    Proc.

handle_msg(Msg, undefined) ->
    ?LOG(error, "Autoheal not enabled! Unexpected msg: ~p", [Msg]), undefined;

handle_msg({report_partition, _Node}, Autoheal = #?MODULE{proc = Proc})
    when Proc =/= undefined ->
    Autoheal;

handle_msg({report_partition, Node}, Autoheal = #?MODULE{timer = TRef}) ->
    case ekka_membership:leader() =:= node() of
        true ->
            ensure_cancel_timer(TRef),
            TRef1 = ekka_node_monitor:run_after(?DELAY, {autoheal, {create_splitview, node()}}),
            Autoheal#?MODULE{role = leader, timer = TRef1};
        false ->
            ?LOG(critical, "I am not leader, but received partition report from ~s", [Node]),
            Autoheal
    end;

handle_msg(Msg = {create_splitview, Node}, Autoheal = #?MODULE{timer = TRef}) when Node =:= node() ->
    ensure_cancel_timer(TRef),
    case ekka_membership:is_all_alive() of
        true ->
            Nodes = ekka_mnesia:cluster_nodes(all),
            case rpc:multicall(Nodes, ekka_mnesia, cluster_view, []) of
                {Views, []} ->
                    SplitView = lists:sort(fun compare_view/2, lists:usort(Views)),
                    ekka_node_monitor:cast(coordinator(SplitView), {heal_partition, SplitView});
                {_Views, BadNodes} ->
                    ?LOG(critical, "Bad nodes found when autoheal: ~p", [BadNodes])
            end,
            Autoheal#?MODULE{timer = undefined};
        false ->
            Autoheal#?MODULE{timer = ekka_node_monitor:run_after(?DELAY, {autoheal, Msg})}
    end;

handle_msg(Msg = {create_splitview, _Node}, Autoheal) ->
    ?LOG(critical, "I am not leader, but received : ~p", [Msg]),
    Autoheal;

handle_msg({heal_partition, SplitView}, Autoheal = #?MODULE{proc = undefined}) ->
    Proc = spawn_link(fun() ->
                          ?LOG(info, "Healing partition: ~p", [SplitView]),
                          HealedNodes = heal_partition(SplitView),
                          ekka_membership:partition_healed(HealedNodes)
                      end),
    Autoheal#?MODULE{role = coordinator, proc = Proc};

handle_msg({heal_partition, SplitView}, Autoheal= #?MODULE{proc = _Proc}) ->
    ?LOG(critical, "Unexpected heal_partition msg: ~p", [SplitView]),
    Autoheal;

handle_msg({'EXIT', Pid, normal}, Autoheal = #?MODULE{proc = Pid}) ->
    Autoheal#?MODULE{proc = undefined};
handle_msg({'EXIT', Pid, Reason}, Autoheal = #?MODULE{proc = Pid}) ->
    ?LOG(critical, "Autoheal process crashed: ~s", [Reason]),
    Autoheal#?MODULE{proc = undefined};

handle_msg(Msg, Autoheal) ->
    ?LOG(critical, "Unexpected msg: ~p", [Msg, Autoheal]),
    Autoheal.

compare_view({Running1, _} , {Running2, _}) ->
    Len1 = length(Running1), Len2 = length(Running2),
    if
        Len1 > Len2  -> true;
        Len1 == Len2 -> lists:member(node(), Running1);
        true -> false
    end.

coordinator([{Nodes, _} | _]) ->
    ekka_membership:coordinator(Nodes).

-spec heal_partition(list()) -> list(node()).
heal_partition([]) ->
    [];
%% All nodes connected.
heal_partition([{_, []}]) ->
    [];
%% Partial partitions happened.
heal_partition([{Nodes, []}|_]) ->
    reboot_minority(Nodes -- [node()]);
heal_partition([{Majority, Minority}, {Minority, Majority}]) ->
    reboot_minority(Minority);
heal_partition(SplitView) ->
    ?LOG(critical, "Cannot heal the partitions: ~p", [SplitView]),
    error({unknown_splitview, SplitView}).

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

