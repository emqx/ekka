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
proc(#autoheal{proc = Proc}) ->
    Proc.

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
                    SplitView = lists:sort(fun compare_view/2, lists:usort(Views)),
                    Coordinator = coordinator(SplitView),
                    ekka_node_monitor:cast(Coordinator, {heal_partition, SplitView}),
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
    case SplitView of
        %% No partitions.
        [] -> Autoheal;
        [{_, []}] -> Autoheal;
        %% Partitions.
        SplitView ->
            Proc = spawn_link(fun() ->
                ?tp(start_heal_partition, #{split_view => SplitView}),
                heal_partition(SplitView)
            end),
            Autoheal#autoheal{role = coordinator, proc = Proc}
    end;

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

compare_view({Running1, _}, {Running2, _}) ->
    Len1 = length(Running1), Len2 = length(Running2),
    if
        Len1 > Len2  -> true;
        Len1 == Len2 -> lists:member(node(), Running1);
        true -> false
    end.

coordinator([{Nodes, _} | _]) ->
    ekka_membership:coordinator(Nodes).

heal_partition([{Nodes, []} | _] = SplitView) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    reboot_minority(Nodes -- [node()]);
heal_partition([{Majority, Minority}, {Minority, Majority}] = SplitView) ->
    %% Symmetric partition.
    ?LOG(info, "Healing partition: ~p", [SplitView]),
    reboot_minority(Minority);
heal_partition([{_Nodes, Stopped} | _Asymmetric] = SplitView) ->
    %% Asymmetric partitions.
    %% Start with rebooting known stopped nodes. If this won't be enough, retry mechanism
    %% in `ekka_node_monitor:handle_info({mnesia_system_event, ...}` should then launch
    %% new iteration.
    ?LOG(info, "Trying to heal asymmetric partition: ~p", [SplitView]),
    reboot_minority(Stopped).

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
