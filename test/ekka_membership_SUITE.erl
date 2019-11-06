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

-module(ekka_membership_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(ekka_mnesia, [non_strict, passthrough, no_history]),
    ok = meck:expect(ekka_mnesia, cluster_status, fun(_) -> running end),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(ekka_mnesia),
    Config.

t_lookup_member(_) ->
    error('TODO').

t_coordinator(_) ->
    error('TODO').

t_ping(_) ->
    error('TODO').

t_pong(_) ->
    error('TODO').

t_node_up(_) ->
    error('TODO').

t_node_down(_) ->
    error('TODO').

t_mnesia_up(_) ->
    error('TODO').

t_mnesia_down(_) ->
    error('TODO').

t_partition_occurred(_) ->
    error('TODO').

t_partition_healed(_) ->
    error('TODO').

t_announce(_) ->
    error('TODO').

t_leader(_) ->
    error('TODO').

t_is_all_alive(_) ->
    error('TODO').

t_oldest(_) ->
    error('TODO').

t_members(_) ->
    error('TODO').

t_monitor(_) ->
    error('TODO').

t_nodelist(_) ->
    error('TODO').

t_is_member(_) ->
    error('TODO').

t_local_member(_) ->
    error('TODO').

with_membership_server(TestFun) ->
    {ok, _} = ekka_membership:start_link(),
    TestFun(),
    ekka_membership:stop().

