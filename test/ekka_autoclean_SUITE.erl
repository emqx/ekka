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

-module(ekka_autoclean_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("ekka.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_name, ekka),
    Previous = application:get_env(ekka, cluster_autoclean),
    ok = application:set_env(ekka, cluster_autoclean, 1000),
    ok = application:set_env(ekka, cluster_discovery, {manual, []}),
    try
        ok = ekka:start(),
        [{autoclean, Previous} | Config]
    catch
        Class:Reason:Stack ->
            restore_autoclean(Previous),
            erlang:raise(Class, Reason, Stack)
    end.

end_per_suite(Config) ->
    try
        application:stop(ekka),
        ekka_mnesia:ensure_stopped()
    after
        restore_autoclean(proplists:get_value(autoclean, Config))
    end.

restore_autoclean({ok, Previous}) -> application:set_env(ekka, cluster_autoclean, Previous);
restore_autoclean(undefined) -> application:unset_env(ekka, cluster_autoclean).

t_autoclean(_) ->
    with_peer(autoclean_expiry, fun(Peer) ->
        {ok, Expiry} = ekka:env(cluster_autoclean),
        Ref = pause_autoclean(Peer),
        BeforeStop = erlang:timestamp(),
        ok = ekka_ct:stop_slave(Peer),
        {Cleaner, Attempt, LTime} = next_attempt(Ref),
        Cleaner ! {Ref, continue},
        %% The deadline is anchored to the local observation of the peer going down, not to
        %% the moment the test stopped it.
        ?assert(timer:now_diff(LTime, BeforeStop) > 0),
        Elapsed = timer:now_diff(Attempt, LTime) div 1000,
        ?assert(Elapsed > Expiry),
        ?assert(Elapsed =< Expiry + Expiry div 4 + 1000),
        wait_removed(Peer)
    end).

t_busy_retries(_) ->
    with_peer(autoclean_busy, fun(Peer) ->
        Ref = pause_autoclean(Peer),
        ok = ekka_ct:stop_slave(Peer),
        {First, _, _} = next_attempt(Ref),
        First ! {Ref, busy},
        {Retry, _, _} = next_attempt(Ref),
        try
            ?assert(lists:member(Peer, ekka_mnesia:cluster_nodes(all)))
        after
            Retry ! {Ref, continue}
        end,
        wait_removed(Peer)
    end).

t_schema_delete_failure_retries(_) ->
    with_peer(autoclean_schema_abort, fun(Peer) ->
        Test = self(),
        Monitor = fun({membership, Event}) -> Test ! Event end,
        ok = ekka:monitor(membership, Monitor),
        Ref = pause_autoclean(Peer),
        Cleaner = whereis(ekka_node_monitor),
        try
            ok = meck:new(mnesia, [passthrough, unstick]),
            ok = meck:expect(mnesia, del_table_copy, fun
                (schema, Node) when Node =:= Peer -> {aborted, schema_delete_failed};
                (Table, Node) -> meck:passthrough([Table, Node])
            end),
            ok = ekka_ct:stop_slave(Peer),
            {Cleaner, _, _} = next_attempt(Ref),
            Cleaner ! {Ref, continue},
            receive {Ref, result, Result} -> ?assertEqual({error, schema_delete_failed}, Result)
            after 5000 -> error(no_removal_result)
            end,
            {Cleaner, _, _} = next_attempt(Ref),
            ?assertEqual(Cleaner, whereis(ekka_node_monitor)),
            ?assertMatch(#member{status = down}, ekka_membership:lookup_member(Peer)),
            ?assert(lists:member(Peer, ekka_mnesia:cluster_nodes(all))),
            receive {node, leaving, Peer} -> error(unexpected_leaving) after 100 -> ok end,
            ok = meck:unload(mnesia),
            Cleaner ! {Ref, continue},
            receive {node, leaving, Peer} -> ok after 5000 -> error(no_leaving) end,
            wait_removed(Peer)
        after
            Cleaner ! {Ref, continue},
            catch meck:unload(mnesia),
            ekka:unmonitor(membership, Monitor)
        end
    end).

t_return_during_autoclean(_) ->
    with_peer(autoclean_return, fun(Peer) ->
        Ref = pause_autoclean(Peer),
        ok = ekka_ct:stop_slave(Peer),
        {Cleaner, _, _} = next_attempt(Ref),
        try
            Peer = ekka_ct:start_slave(ekka, autoclean_return),
            ok = ekka_ct:wait_running(Peer),
            ?assert(lists:member(Peer, ekka_mnesia:running_nodes()))
        after
            Cleaner ! {Ref, continue}
        end,
        receive {Ref, result, Result} -> ?assertEqual(ignore, Result)
        after 10000 -> error(autoclean_not_resumed)
        end,
        ?assert(lists:member(Peer, ekka_mnesia:cluster_nodes(all))),
        ?assert(lists:member(node(), rpc:call(Peer, ekka_mnesia, running_nodes, []))),
        ?assert(ekka:is_running(Peer, ekka))
    end).

pause_autoclean(Peer) ->
    Test = self(),
    Ref = make_ref(),
    ok = ekka:callback(membership_guard, fun
        (Node, [], Action) when Node =:= Peer ->
            #member{ltime = LTime} = ekka_membership:lookup_member(Node),
            Test ! {Ref, attempt, self(), erlang:timestamp(), LTime},
            receive
                {Ref, busy} -> {error, busy};
                {Ref, continue} ->
                    Result = Action(),
                    Test ! {Ref, result, Result},
                    Result
            after 30000 -> {error, timeout}
            end;
        (_, _, Action) -> Action()
    end),
    Ref.

next_attempt(Ref) ->
    receive {Ref, attempt, Pid, Time, LTime} -> {Pid, Time, LTime}
    after 10000 -> error(autoclean_not_attempted)
    end.

with_peer(Name, Test) ->
    Peer = ekka_ct:start_slave(ekka, Name),
    try
        ok = rpc:call(Peer, ekka_cluster, join, [node()]),
        ?assert(lists:member(Peer, ekka_mnesia:running_nodes())),
        Test(Peer)
    after
        application:unset_env(ekka, {callback, membership_guard}),
        try
            case lists:member(Peer, ekka_mnesia:running_nodes()) of
                true -> rpc:call(Peer, ekka_cluster, leave, []);
                false -> ok
            end
        after
            ekka_ct:stop_slave(Peer)
        end
    end.

wait_removed(Peer) -> wait_removed(Peer, 100).

wait_removed(Peer, 0) ->
    ?assertNot(lists:member(Peer, ekka_mnesia:cluster_nodes(all)));
wait_removed(Peer, Tries) ->
    case lists:member(Peer, ekka_mnesia:cluster_nodes(all)) of
        false -> ok;
        true -> timer:sleep(50), wait_removed(Peer, Tries - 1)
    end.
