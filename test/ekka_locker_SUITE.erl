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

-module(ekka_locker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("mria/include/mria.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SERVER, locker_server).

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(mria_membership, [non_strict, passthrough, no_history]),
    {ok, _Locker} = ekka_locker:start_link(?SERVER),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = ekka_locker:stop(locker_server),
    ok = meck:unload(mria_membership),
    Config.

t_acquire_release_local(_) ->
    Node = node(),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource)),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource)).

t_acquire_release_leader(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, leader, fun() -> Node end),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, leader)),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, leader)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, leader)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, leader)).

t_acquire_release_quorum(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, ring, fun(_) -> [#member{node = Node, status = up}] end),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, quorum)),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, quorum)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, quorum)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, quorum)).

t_acquire_release_all(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [Node] end),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, all)),
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, all)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, all)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, all)).

t_acquire_should_fail_with_empty_nodelist(_) ->
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [] end),
    ?assertEqual([], ets:tab2list(?SERVER)),
    ?assertEqual({false, []}, ekka_locker:acquire(?SERVER, resource, all)),
    ?assertEqual([], ets:tab2list(?SERVER)).

t_acquire_should_fail_when_all_nodes_down(_) ->
    Node = 'node@nowhere',
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [Node] end),
    ?assertEqual([], ets:tab2list(?SERVER)),
    ?assertEqual({false, []}, ekka_locker:acquire(?SERVER, resource, all)),
    ?assertEqual([], ets:tab2list(?SERVER)).

t_acquire_should_fail_when_rpc_fails(_) ->
    Remote = 'node@nowhere',
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [node(), Remote] end),
    ok = meck:new(erpc, [unstick, passthrough]),
    %% the remote node fails, the local node runs the real call
    ok = meck:expect(erpc, multicall,
                     fun(Nodes, M, F, A, Timeout) ->
                             [case N of
                                  Remote -> {error, {exception, badarg, []}};
                                  _ -> hd(meck:passthrough([[N], M, F, A, Timeout]))
                              end || N <- Nodes]
                     end),
    ?assertEqual([], ets:tab2list(?SERVER)),
    ?assertMatch({false, [{badrpc, {'EXIT', {badarg, []}}}]},
                 ekka_locker:acquire(?SERVER, resource, all)),
    %% the lock taken on the local node is released
    ?assertEqual([], ets:tab2list(?SERVER)),
    %% one call for acquire, one for the release after the failure
    ?assertEqual(2, meck:num_calls(erpc, multicall, '_')),
    %% with more than one node, the local node is in the multicall too
    ?assert(meck:called(erpc, multicall, [[node(), Remote], '_', '_', '_', '_'])),
    ok = meck:unload(erpc).

t_no_rpc_to_local_node(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, ring, fun(_) -> [#member{node = Node, status = up}] end),
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [Node] end),
    ok = meck:new(erpc, [unstick, passthrough]),
    lists:foreach(
      fun(Type) ->
              ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, Type)),
              ?assertMatch([_], ets:tab2list(?SERVER)),
              ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, Type)),
              ?assertEqual([], ets:tab2list(?SERVER))
      end, [quorum, all]),
    ?assertEqual(0, meck:num_calls(erpc, multicall, '_')),
    ok = meck:unload(erpc).

t_acquire_release_all_with_down_node(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [Node, 'node@nowhere'] end),
    Self = self(),
    %% a down node is a bad node of the multicall, it is ignored
    ?assertEqual({true, [Node]}, ekka_locker:acquire(?SERVER, resource, all)),
    ?assertMatch([{lock, resource, Self, 1, _}], ets:tab2list(?SERVER)),
    ?assertEqual({true, [Node]}, ekka_locker:release(?SERVER, resource, all)),
    ?assertEqual([], ets:tab2list(?SERVER)).

t_acquire_should_fail_when_piggyback_crashes(_) ->
    Node = node(),
    ok = meck:expect(mria_membership, nodelist, fun(_) -> [Node] end),
    Piggyback = {erlang, error, [boom]},
    ?assertMatch({false, [{badrpc, {'EXIT', {boom, _}}}]},
                 ekka_locker:acquire(?SERVER, resource, all, Piggyback)),
    ?assertEqual([], ets:tab2list(?SERVER)),
    ?assertMatch({false, [{badrpc, {'EXIT', killed}}]},
                 ekka_locker:acquire(?SERVER, resource, all, {erlang, exit, [killed]})),
    ?assertEqual([], ets:tab2list(?SERVER)).
