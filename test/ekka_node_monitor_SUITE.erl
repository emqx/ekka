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

-module(ekka_node_monitor_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(MOCK_STATE, {?MODULE, mock_state}).

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_discovery, {manual, []}),
    Autoheal = application:get_env(ekka, cluster_autoheal),
    ok = application:set_env(ekka, cluster_autoheal, false),
    application:ensure_all_started(ekka),
    %% Earlier suites can leave three-second confirmation timers addressed to the
    %% registered name. Let them expire with an empty partition list before testing.
    ok = clear_partitions(),
    timer:sleep(3100),
    [] = ekka_node_monitor:partitions(),
    [{cluster_autoheal, Autoheal} | Config].

end_per_suite(Config) ->
    case proplists:get_value(cluster_autoheal, Config) of
        undefined -> application:unset_env(ekka, cluster_autoheal);
        {ok, Autoheal} -> application:set_env(ekka, cluster_autoheal, Autoheal)
    end,
    ok = application:stop(ekka),
    ekka_mnesia:ensure_stopped().

init_per_testcase(_TestCase, Config) ->
    %% Clear partitions before a case arms anything. Autoheal stays off for the
    %% clear so the mnesia_up handler does not schedule another confirmation.
    ok = application:set_env(ekka, cluster_autoheal, false),
    ok = clear_partitions(),
    persistent_term:put(?MOCK_STATE, #{leader => node(), leader_up => true}),
    ok = install_mocks(),
    ok = application:set_env(ekka, cluster_autoheal, true),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = application:set_env(ekka, cluster_autoheal, false),
    ok = clear_partitions(),
    ok = meck:unload([ekka_membership, ekka_node, ekka_autoheal]),
    _ = persistent_term:erase(?MOCK_STATE),
    ok.

t_cast_heartbeat(_) ->
    ok = ekka_node_monitor:cast(node(), heartbeat).

t_cast_suspect(_) ->
    ok = ekka_node_monitor:cast(node(), {suspect, 'n1@127.0.0.1', 'n2@127.0.0.1'}).

t_cast_confirm(_) ->
    ok = ekka_node_monitor:cast(node(), {confirm, 'n1@127.0.0.1', down}).

t_cast_report_partition(_) ->
    ok = ekka_node_monitor:cast(node, {report_partition, 'n1@127.0.0.1'}).

t_cast_heal_partition(_) ->
    ok = ekka_node_monitor:cast(node, {heal_partition, ['n1@127.0.0.1']}).

t_handle_nodeup_info(_) ->
    ekka_node_monitor ! {nodeup, 'n1@127.0.0.1', []}.

t_handle_nodedown_info(_) ->
    ekka_node_monitor ! {nodedown, 'n1@127.0.0.1', []}.

t_run_after(_) ->
    TRef = ekka_node_monitor:run_after(100, heartbeat),
    ?assert(is_reference(TRef)).

t_partitions(_) ->
    [] = ekka_node_monitor:partitions().

t_handle_unexpected(_) ->
    {reply, ignore, state} = ekka_node_monitor:handle_call(req, from, state),
    {noreply, state} = ekka_node_monitor:handle_cast(msg, state),
    {noreply, state} = ekka_node_monitor:handle_info(info, state).

%%--------------------------------------------------------------------
%% Partition confirmation retries
%%--------------------------------------------------------------------

t_confirm_retries_until_leader_ekka_available(_Config) ->
    ok = set_mocks(#{leader_up => false}),
    inject_partition(node()),
    receive {is_running, _, ekka, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
    FirstConfirm = erlang:monotonic_time(millisecond),
    ok = set_mocks(#{leader_up => true}),
    receive {is_running, _, ekka, true} -> ok after 5000 -> ct:fail(no_retry_confirm) end,
    %% The confirmation must wait for the retry timer, not fire immediately.
    ?assert(erlang:monotonic_time(millisecond) - FirstConfirm >= 2000),
    receive
        {autoheal_msg, {report_partition, Node}} ->
            ?assertEqual(node(), Node)
    after 2000 -> ct:fail(no_report_after_leader_recovery)
    end,
    receive {is_running, _, ekka, _} -> ct:fail(confirm_after_report)
    after 4000 -> ok
    end.

t_confirm_skipped_when_autoheal_disabled(_Config) ->
    inject_partition(node()),
    ok = wait_until(fun() -> ekka_node_monitor:partitions() =:= [node()] end, 50),
    ok = application:set_env(ekka, cluster_autoheal, false),
    receive {is_running, _, ekka, _} -> ct:fail(confirmed_despite_autoheal_disabled)
    after 4000 -> ok
    end,
    receive
        {autoheal_msg, {report_partition, _}} -> ct:fail(reported_despite_autoheal_disabled)
    after 0 -> ok
    end.

t_confirm_retry_stops_when_partition_cleared(_Config) ->
    ok = set_mocks(#{leader_up => false}),
    inject_partition(node()),
    receive {is_running, _, ekka, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
    ekka_node_monitor ! {mnesia_system_event, {mnesia_up, node()}},
    ok = wait_until(fun() -> ekka_node_monitor:partitions() =:= [] end, 50),
    ok = set_mocks(#{leader_up => true}),
    receive
        {autoheal_msg, {report_partition, _}} -> ct:fail(report_after_partition_cleared)
    after 4000 -> ok
    end,
    receive {is_running, _, ekka, _} -> ct:fail(confirm_after_partition_cleared)
    after 0 -> ok
    end.

t_confirm_retry_stops_when_autoheal_disabled(_Config) ->
    ok = set_mocks(#{leader_up => false}),
    inject_partition(node()),
    receive {is_running, _, ekka, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
    ok = application:set_env(ekka, cluster_autoheal, false),
    ok = set_mocks(#{leader_up => true}),
    receive
        {autoheal_msg, {report_partition, _}} -> ct:fail(report_after_autoheal_disabled)
    after 4000 -> ok
    end,
    receive {is_running, _, ekka, _} -> ct:fail(confirm_after_autoheal_disabled)
    after 0 -> ok
    end.

t_confirm_retry_uses_current_leader(_Config) ->
    set_mocks(#{leader => 'fake-leader@127.0.0.1', leader_up => false}),
    inject_partition(node()),
    receive {is_running, _, ekka, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
    ok = set_mocks(#{leader => node(), leader_up => true}),
    receive
        {autoheal_msg, {report_partition, Node}} ->
            ?assertEqual(node(), Node)
    after 6000 -> ct:fail(no_report_to_new_leader)
    end.

t_confirm_retry_coalesced(_Config) ->
    ok = set_mocks(#{leader_up => false}),
    inject_partition(node()),
    inject_partition(node()),
    ok = wait_for_confirm_calls(2, 6000),
    ok = set_mocks(#{leader_up => true}),
    receive
        {autoheal_msg, {report_partition, Node}} ->
            ?assertEqual(node(), Node)
    after 6000 -> ct:fail(no_report)
    end,
    receive
        {autoheal_msg, {report_partition, _}} -> ct:fail(duplicate_report)
    after 2000 -> ok
    end.

t_confirm_ignores_queued_retry_after_report(_Config) ->
    ok = set_mocks(#{leader_up => false}),
    inject_partition(node()),
    receive {is_running, _, ekka, false} -> ok after 5000 -> ct:fail(no_first_confirm) end,
    %% Suspend between callbacks, after the failed confirmation has armed its retry.
    ok = sys:suspend(ekka_node_monitor),
    try
        ok = set_mocks(#{leader_up => true}),
        ekka_node_monitor ! confirm_partition,
        %% Let the three-second retry expire behind the ordinary confirmation.
        timer:sleep(3500)
    after
        ok = sys:resume(ekka_node_monitor)
    end,
    receive {autoheal_msg, {report_partition, _}} -> ok after 2000 -> ct:fail(no_report) end,
    %% Drain any second report already cast to the monitor itself.
    _ = ekka_node_monitor:partitions(),
    receive {autoheal_msg, {report_partition, _}} -> ct:fail(duplicate_report)
    after 0 -> ok
    end.

install_mocks() ->
    Test = self(),
    ok = meck:new(ekka_autoheal, [passthrough, no_link]),
    ok = meck:expect(ekka_autoheal, handle_msg, fun(Msg, Autoheal) ->
        Test ! {autoheal_msg, Msg},
        Autoheal
    end),
    ok = meck:new(ekka_node, [passthrough, no_link]),
    ok = meck:expect(ekka_node, is_running, fun(Node, App) ->
        Result = case App =:= ekka andalso (Node =:= node() orelse Node =:= mock(leader)) of
            true -> mock(leader_up);
            false -> meck:passthrough([Node, App])
        end,
        Test ! {is_running, Node, App, Result},
        Result
    end),
    ok = meck:new(ekka_membership, [passthrough, no_link]),
    ok = meck:expect(ekka_membership, leader, fun() -> mock(leader) end),
    ok.

set_mocks(Values) ->
    ok = persistent_term:put(?MOCK_STATE, maps:merge(mock_state(), Values)).

mock(Key) ->
    maps:get(Key, mock_state()).

mock_state() ->
    persistent_term:get(?MOCK_STATE).

inject_partition(Node) ->
    ekka_node_monitor ! {mnesia_system_event, {inconsistent_database, undefined, Node}}.

clear_partitions() ->
    ekka_node_monitor ! {mnesia_system_event, {mnesia_up, node()}},
    wait_until(fun() -> ekka_node_monitor:partitions() =:= [] end, 50).

wait_for_confirm_calls(0, _Timeout) ->
    ok;
wait_for_confirm_calls(N, Timeout) ->
    receive
        {is_running, _, ekka, false} -> wait_for_confirm_calls(N - 1, Timeout)
    after Timeout -> ct:fail({missing_confirm_calls, N})
    end.

wait_until(_Fun, 0) ->
    ct:fail(wait_timeout);
wait_until(Fun, Retries) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(50),
            wait_until(Fun, Retries - 1)
    end.

