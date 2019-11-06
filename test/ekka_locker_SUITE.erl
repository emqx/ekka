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

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(ekka_membership, [non_strict, passthrough, no_history]),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(ekka_membership),
    Config.

t_start_stop(_) ->
    {ok, _Locker} = ekka_locker:start_link(),
    ekka_locker:stop().

t_acquire_release_local(_) ->
    with_locker_server(
      fun(Locker) ->
              Node = node(),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource)),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource))
      end).

t_acquire_release_leader(_) ->
    with_locker_server(
      fun(Locker) ->
              Node = node(),
              ok = meck:expect(ekka_membership, leader, fun() -> Node end),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, leader)),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, leader)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, leader)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, leader))
      end).

t_acquire_release_quorum(_) ->
    with_locker_server(
      fun(Locker) ->
              Node = node(),
              ok = meck:expect(ekka_membership, ring, fun(_) -> [Node] end),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, quorum)),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, quorum)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, quorum)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, quorum))
      end).

t_acquire_release_all(_) ->
    with_locker_server(
      fun(Locker) ->
              Node = node(),
              ok = meck:expect(ekka_membership, nodelist, fun(_) -> [Node] end),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, all)),
              ?assertEqual({true, [Node]}, ekka_locker:acquire(test_locker, resource, all)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, all)),
              ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource, all))
      end).

with_locker_server(TestFun) ->
    {ok, Locker} = ekka_locker:start_link(test_locker),
    TestFun(Locker),
    ekka_locker:stop(Locker).

