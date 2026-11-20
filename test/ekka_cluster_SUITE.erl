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

-module(ekka_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("ekka.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_name, ekka),
    ok = application:set_env(ekka, cluster_discovery, {manual, []}),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped().

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_join_leave(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    N2 = ekka_ct:start_slave(ekka, n2),
    try
        ekka_ct:wait_running(N1),
        ekka_ct:wait_running(N2),
        true = ekka:is_running(N1, ekka),
        true = ekka:is_running(N2, ekka),
        ok = rpc:call(N1, ekka_cluster, join, [N0]),
        ok = rpc:call(N2, ekka_cluster, join, [N0]),
        [N0, N1, N2] = ekka_cluster:info(running_nodes),
        Busy = fun(_, _, _) -> {error, busy} end,
        ok = rpc:call(N1, ekka, callback, [membership_guard, Busy]),
        ?assertEqual({error, busy}, rpc:call(N1, ekka_cluster, leave, [])),
        ?assertEqual(running, ekka_cluster:status(N1)),
        ok = rpc:call(N1, ekka, callback, [membership_guard, fun(_, _, Action) -> Action() end]),
        ok = rpc:call(N1, ekka_cluster, leave, []),
        ok = rpc:call(N2, ekka_cluster, leave, []),
        [N0] = ekka_cluster:info(running_nodes)
    after
        ok = ekka_ct:stop_slave(N1),
        ok = ekka_ct:stop_slave(N2)
    end.

t_join_rechecks_membership(_) ->
    Target = node(),
    Peer = ekka_ct:start_slave(ekka, n1),
    Parent = self(),
    Guard = fun(_, Targets, Continue) ->
        case Targets of
            [Target] ->
                Parent ! {joining, self()},
                receive proceed -> Continue() after 10000 -> {error, timeout} end;
            [] -> Continue()
        end
    end,
    try
        ok = rpc:call(Peer, ekka, callback, [membership_guard, Guard]),
        First = rpc:async_call(Peer, ekka_cluster, join, [Target]),
        Pid1 = receive {joining, P1} -> P1 after 5000 -> error(first_join) end,
        Second = rpc:async_call(Peer, ekka_cluster, join, [Target]),
        Pid2 = receive {joining, P2} -> P2 after 5000 -> error(second_join) end,
        Pid1 ! proceed,
        ?assertEqual(ok, rpc:yield(First)),
        Pid2 ! proceed,
        ?assertEqual({error, {already_in_cluster, Target}}, rpc:yield(Second)),
        ?assertEqual(running, ekka_cluster:status(Peer)),
        ok = ekka_cluster:force_leave(Peer)
    after
        ekka_ct:stop_slave(Peer)
    end.

t_force_leave(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    N2 = ekka_ct:start_slave(ekka, n2),
    try
        ok = ekka_ct:wait_running(N1),
        ok = ekka_ct:wait_running(N2),
        true = ekka:is_running(N1, ekka),
        true = ekka:is_running(N2, ekka),
        ok = rpc:call(N1, ekka_cluster, join, [N0]),
        ok = rpc:call(N2, ekka_cluster, join, [N1]),
        [N0, N1, N2] = ekka_cluster:info(running_nodes),
        ok = ekka_cluster:force_leave(N1),
        ok = ekka_cluster:force_leave(N2),
        [N0] = ekka_cluster:info(running_nodes)
    after
        ok = ekka_ct:stop_slave(N1),
        ok = ekka_ct:stop_slave(N2)
    end.

t_prepare(_) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    ok = rpc:call(N1, ekka_cluster, prepare, [join]),
    ok = ekka_ct:stop_slave(N1).

t_reboot(_) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    ok = rpc:call(N1, ekka_cluster, reboot, []),
    ok = ekka_ct:stop_slave(N1).

t_status(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        true = ekka:is_running(N1, ekka),
        ok = rpc:call(N1, ekka_cluster, join, [N0]),
        running = ekka_cluster:status(N1),
        ok = ekka_cluster:force_leave(N1),
        false = ekka_cluster:status(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% A membership change ends with an application restart that re-enters the membership
%% guard. A transient refusal of that second admission must be retried, and the mnesia
%% stop/start must not be repeated.
%%--------------------------------------------------------------------

t_heal_retries_application_start(_) ->
    Node = ekka_ct:start_slave(ekka, retry_reboot),
    try
        install_prepare_counter(Node),
        %% The first admission (mnesia stop/start) is allowed; the application start is
        %% rejected once, then allowed.
        install_guard(Node, fun(Action) ->
            case bump(guard_calls) of
                2 -> {error, {route_cleanup_busy, Node}};
                _ -> Action()
            end
        end),
        ?assertEqual(ok, rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 60000)),
        ?assertEqual(1, counter(Node, prepares)),
        ?assertEqual(3, counter(Node, guard_calls)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assert(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

t_heal_application_start_gives_up(_) ->
    Node = ekka_ct:start_slave(ekka, retry_give_up),
    try
        install_prepare_counter(Node),
        install_guard(Node, fun(Action) ->
            case bump(guard_calls) of
                1 -> Action();
                _ -> {error, {route_cleanup_busy, Node}}
            end
        end),
        ?assertMatch({error, _}, rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 60000)),
        %% One attempt plus the bounded retries; the mnesia stop/start happened once.
        ?assertEqual(5, counter(Node, guard_calls)),
        ?assertEqual(1, counter(Node, prepares)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assertNot(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

%%--------------------------------------------------------------------
%% A refusal happens before the local application is stopped, so it is reported separately
%% and nothing has to be undone; an error returned after the stop/start ran keeps its own
%% shape and is not retried.
%%--------------------------------------------------------------------

t_heal_reports_guard_refusal(_) ->
    Node = ekka_ct:start_slave(ekka, heal_refused),
    try
        install_prepare_counter(Node),
        install_guard(Node, fun(_Action) -> {error, {route_cleanup_busy, Node}} end),
        ?assertEqual({error, {guard_refused, {route_cleanup_busy, Node}}},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 60000)),
        ?assertEqual(0, counter(Node, prepares)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assert(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

t_heal_action_error_is_not_retried(_) ->
    Node = ekka_ct:start_slave(ekka, heal_action_error),
    try
        install_prepare_counter(Node),
        %% The guard runs the action (the change started) and then reports an error.
        install_guard(Node, fun(Action) -> _ = Action(), {error, simulated} end),
        ?assertEqual({error, simulated},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 60000)),
        %% The change ran once and is not retried; the application stage was not reached.
        ?assertEqual(1, counter(Node, prepares)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assertNot(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

t_guard_action_in_worker(_) ->
    Guard = fun(_, _, Action) ->
        {Pid, Ref} = spawn_monitor(fun() -> exit({action_result, Action()}) end),
        receive
            {'DOWN', Ref, process, Pid, Reason} ->
                {action_result, Result} = Reason,
                Result
        end
    end,
    Previous = ekka:callback(membership_guard),
    ok = ekka:callback(membership_guard, Guard),
    try
        ?assertEqual({error, busy}, ekka_mnesia:guarded_refusal(node(), [], fun() -> {error, busy} end))
    after
        case Previous of
            undefined -> application:unset_env(ekka, {callback, membership_guard});
            {ok, Old} -> ekka:callback(membership_guard, Old)
        end
    end.

t_heal_only_tags_refusals_before_the_action(_) ->
    Node = ekka_ct:start_slave(ekka, heal_refusal_tag),
    try
        Refusal = {guard_refused, busy},
        ok = rpc:call(Node, ekka, callback, [prepare, fun(_) -> {error, Refusal} end]),
        ?assertEqual({error, {membership_action_failed, Refusal}},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot])),
        install_prepare_counter(Node),
        install_guard(Node, fun(Action) -> _ = Action(), {error, Refusal} end),
        ?assertEqual({error, {membership_action_failed, Refusal}},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot])),
        ?assertEqual(1, counter(Node, prepares))
    after
        ekka_ct:stop_slave(Node)
    end.

t_heal_does_not_retry_thrown_application_refusal(_) ->
    Node = ekka_ct:start_slave(ekka, heal_throw_reboot),
    try
        install_prepare_counter(Node),
        Refusal = {guard_refused, busy},
        Reboot = fun() ->
            _ = bump(reboots),
            throw({error, Refusal})
        end,
        ok = rpc:call(Node, ekka, callback, [reboot, Reboot]),
        ?assertEqual({error, {application_restart_failed, Refusal}},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot])),
        ?assertEqual(1, counter(Node, prepares)),
        ?assertEqual(1, counter(Node, reboots)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assertNot(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

%% Only a known transient refusal is retried: a permanent refusal of the application
%% start is returned after a single attempt.
t_heal_permanent_application_refusal_is_not_retried(_) ->
    Node = ekka_ct:start_slave(ekka, retry_permanent),
    try
        install_prepare_counter(Node),
        install_guard(Node, fun(Action) ->
            case bump(guard_calls) of
                1 -> Action();
                _ -> {error, not_allowed}
            end
        end),
        ?assertEqual({error, {application_restart_failed, {guard_refused, not_allowed}}},
                     rpc:call(Node, ekka_cluster, heal, [shutdown_and_reboot], 60000)),
        %% The mnesia stop/start happened once, the application start was attempted once.
        ?assertEqual(1, counter(Node, prepares)),
        ?assertEqual(2, counter(Node, guard_calls)),
        ?assertEqual(yes, rpc:call(Node, mnesia, system_info, [is_running])),
        ?assertNot(ekka:is_running(Node, ekka))
    after
        ekka_ct:stop_slave(Node)
    end.

%%--------------------------------------------------------------------
%% An online force-leave commits the schema change first; only the application start of the
%% target is retried when the guard refuses it.
%%--------------------------------------------------------------------

t_force_leave_online_retries_application_start(_) ->
    Target = ekka_ct:start_slave(ekka, leave_online),
    try
        ok = rpc:call(Target, ekka_cluster, join, [node()]),
        install_prepare_counter(Target),
        %% The membership change runs on this node; the target's first admission is its
        %% application start, which is refused once.
        install_guard(Target, fun(Action) ->
            case bump(guard_calls) of
                1 -> {error, {route_cleanup_busy, Target}};
                _ -> Action()
            end
        end),
        ?assertEqual(ok, ekka_cluster:force_leave(Target)),
        ?assertEqual(1, counter(Target, prepares)),
        ?assertEqual(2, counter(Target, guard_calls)),
        ?assertEqual(yes, rpc:call(Target, mnesia, system_info, [is_running])),
        ?assert(ekka:is_running(Target, ekka))
    after
        catch ekka_ct:stop_slave(Target)
    end.

%%--------------------------------------------------------------------
%% The join target can go down while the join waits inside the guard, before prepare stops
%% the local application.
%%--------------------------------------------------------------------

t_join_rechecks_target_availability(_) ->
    Target = ekka_ct:start_slave(ekka, join_target),
    Joiner = ekka_ct:start_slave(ekka, join_joiner),
    Parent = self(),
    Guard = fun(_Node, Targets, Action) ->
        case Targets of
            [Target] ->
                Parent ! {joining, self()},
                receive proceed -> Action() after 30000 -> {error, timeout} end;
            [] -> Action()
        end
    end,
    try
        ok = ekka_ct:wait_running(Target),
        ok = ekka_ct:wait_running(Joiner),
        ok = rpc:call(Joiner, ekka, callback, [membership_guard, Guard]),
        Join = rpc:async_call(Joiner, ekka_cluster, join, [Target]),
        Pid = receive {joining, P} -> P after 5000 -> error(join_not_guarded) end,
        ok = ekka_ct:stop_slave(Target),
        Pid ! proceed,
        ?assertEqual({error, {node_down, Target}}, rpc:yield(Join)),
        ?assert(ekka:is_running(Joiner, ekka)),
        ?assertEqual([Joiner], rpc:call(Joiner, ekka_mnesia, cluster_nodes, [all]))
    after
        catch ekka_ct:stop_slave(Joiner),
        catch ekka_ct:stop_slave(Target)
    end.

t_join_bounds_the_initial_probe(_) ->
    Target = ekka_ct:start_slave(ekka, join_unresponsive),
    Test = self(),
    try
        ok = rpc:call(Target, meck, new, [ekka_node, [passthrough, no_link]]),
        ok = rpc:call(Target, meck, expect, [ekka_node, is_running, fun
            (ekka) ->
                Test ! probe,
                receive release_probe -> true after 15000 -> false end;
            (App) ->
                meck:passthrough([App])
        end]),
        ?assertEqual(pong, net_adm:ping(Target)),
        Schema = ekka_mnesia:cluster_nodes(all),
        Monitor = whereis(ekka_node_monitor),
        ?assertEqual({error, {node_down, Target}},
                     rpc:call(node(), ekka_cluster, join, [Target], 7500)),
        receive probe -> ok after 1000 -> error(no_probe) end,
        ?assertEqual(Schema, ekka_mnesia:cluster_nodes(all)),
        ?assertEqual(Monitor, whereis(ekka_node_monitor)),
        ok = rpc:call(Target, ekka_cluster, join, [node()]),
        ?assertEqual({error, {already_in_cluster, Target}},
                     rpc:call(node(), ekka_cluster, join, [Target], 1000)),
        receive probe -> error(unexpected_probe) after 100 -> ok end,
        ?assertEqual(Monitor, whereis(ekka_node_monitor)),
        ok = ekka_cluster:force_leave(Target)
    after
        ekka_ct:stop_slave(Target)
    end.

%%--------------------------------------------------------------------
%% An offline force-leave notifies the membership only after the schema deletion
%% succeeded, and not at all when the deletion failed.
%%--------------------------------------------------------------------

t_force_leave_offline_notifies_after_removal(_) ->
    Peer = ekka_ct:start_slave(ekka, leave_notify),
    Monitor = monitor_fun(),
    try
        ok = rpc:call(Peer, ekka_cluster, join, [node()]),
        ok = ekka:monitor(membership, Monitor),
        _ = wait_member(Peer, up),
        ok = stop_offline_peer(Peer),
        _ = wait_member(Peer, down),
        ?assert(lists:member(Peer, ekka_mnesia:cluster_nodes(all))),
        ?assertEqual(ok, ekka_cluster:force_leave(Peer)),
        receive {membership_event, {node, leaving, Peer}, NodesAtNotify} ->
            %% The view is captured inside the membership server, when the notification is
            %% produced: it must not be announced before the schema deletion.
            ?assertNot(lists:member(Peer, NodesAtNotify))
        after 5000 -> error(no_leaving_notification)
        end,
        ?assertEqual(false, ekka_membership:lookup_member(Peer))
    after
        catch ekka:unmonitor(membership, Monitor),
        cleanup_offline_peer(Peer)
    end.

t_force_leave_offline_failure_keeps_member(_) ->
    Peer = ekka_ct:start_slave(ekka, leave_failure),
    Parent = self(),
    Monitor = monitor_fun(),
    try
        ok = rpc:call(Peer, ekka_cluster, join, [node()]),
        ok = ekka:monitor(membership, Monitor),
        _ = wait_member(Peer, up),
        ok = stop_offline_peer(Peer),
        Down = wait_member(Peer, down),
        ok = meck:new(mnesia, [passthrough, unstick]),
        ok = meck:expect(mnesia, del_table_copy, fun
            (schema, N) when N =:= Peer ->
                Parent ! {removing, N},
                receive proceed -> {aborted, simulated_removal_failure} end;
            (Table, N) ->
                meck:passthrough([Table, N])
        end),
        {Caller, Ref} = spawn_monitor(fun() ->
            Parent ! {removal_result, ekka_cluster:force_leave(Peer)}
        end),
        try
            receive {removing, Peer} -> ok after 5000 -> error(removal_not_started) end,
            %% No notification while the deletion is still pending.
            ?assertNot(receive_event({node, leaving, Peer}, 300)),
            Caller ! proceed,
            receive {removal_result, Result} ->
                ?assertEqual({error, simulated_removal_failure}, Result)
            after 5000 -> error(no_removal_result)
            end
        after
            exit(Caller, kill),
            receive {'DOWN', Ref, process, Caller, _} -> ok
            after 5000 -> error({test_removal_caller_still_running, Peer})
            end
        end,
        ?assertNot(receive_event({node, leaving, Peer}, 500)),
        ?assertEqual(Down, peer_record(Peer)),
        ?assert(lists:member(Peer, ekka_mnesia:cluster_nodes(all)))
    after
        catch meck:unload(mnesia),
        catch ekka:unmonitor(membership, Monitor),
        cleanup_offline_peer(Peer)
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

stop_offline_peer(Peer) ->
    ok = ekka_ct:stop_slave(Peer),
    %% The membership-down notification can arrive before Mnesia observes the stop.
    wait_mnesia_down(Peer, 100).

wait_mnesia_down(Peer, Tries) ->
    Running = ekka_mnesia:running_nodes(),
    case lists:member(Peer, Running) of
        false -> ok;
        true when Tries > 0 ->
            timer:sleep(50),
            wait_mnesia_down(Peer, Tries - 1);
        true -> error({test_peer_mnesia_still_running, Peer, Running})
    end.

cleanup_offline_peer(Peer) ->
    ok = stop_offline_peer(Peer),
    Result = case ekka_mnesia:is_node_in_cluster(Peer) of
        true -> ekka_cluster:force_leave(Peer);
        false -> ok
    end,
    Schema = ekka_mnesia:cluster_nodes(all),
    case {Result, lists:member(Peer, Schema)} of
        {ok, false} -> ok;
        {{error, node_not_in_cluster}, false} -> ok;
        _ -> error({test_peer_cleanup_failed, Peer, Result, Schema})
    end.

install_guard(Node, Decide) ->
    Guard = fun(_Node, _Targets, Action) -> Decide(Action) end,
    ok = rpc:call(Node, ekka, callback, [membership_guard, Guard]).

install_prepare_counter(Node) ->
    Prepare = fun(_Action) ->
        _ = bump(prepares),
        application:stop(ekka)
    end,
    ok = rpc:call(Node, ekka, callback, [prepare, Prepare]).

install_reboot_counter(Node) ->
    Reboot = fun() ->
        _ = bump(reboots),
        ekka:start()
    end,
    ok = rpc:call(Node, ekka, callback, [reboot, Reboot]).

%% Counters are kept in the ekka application environment of the node under test.
bump(Key) ->
    N = application:get_env(ekka, {?MODULE, Key}, 0) + 1,
    ok = application:set_env(ekka, {?MODULE, Key}, N),
    N.

counter(Node, Key) ->
    rpc:call(Node, application, get_env, [ekka, {?MODULE, Key}, 0]).

monitor_fun() ->
    Test = self(),
    fun({membership, Event}) ->
        Test ! {membership_event, Event, ekka_mnesia:cluster_nodes(all)}
    end.

receive_event(Event, Timeout) ->
    receive
        {membership_event, Event, _Nodes} -> true;
        {membership_event, _Other, _Nodes} -> receive_event(Event, Timeout)
    after Timeout -> false
    end.

wait_member(Node, Status) ->
    wait_member(Node, Status, 100).

wait_member(Node, Status, 0) ->
    error({member_not_in_status, Node, Status, peer_record(Node)});
wait_member(Node, Status, Tries) ->
    case peer_record(Node) of
        #member{status = Status} = Member -> Member;
        _ -> timer:sleep(50), wait_member(Node, Status, Tries - 1)
    end.

peer_record(Node) ->
    ekka_membership:lookup_member(Node).
