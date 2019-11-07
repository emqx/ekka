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

-module(ekka_autoheal_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    application:set_env(ekka, cluster_autoheal, true),
    ok = meck:new(ekka_membership, [non_strict, passthrough, no_history]),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(ekka_membership),
    application:unset_env(ekka, cluster_autoheal),
    Config.

t_init(_) ->
    ?assertEqual(autoheal, element(1, ekka_autoheal:init())).

t_enabled(_) ->
    ?assert(ekka_autoheal:enabled()).

t_proc(_) ->
    undefined = ekka_autoheal:proc(ekka_autoheal:init()).

t_handle_report_partition_msg(_) ->
    ok = meck:expect(ekka_membership, leader, fun() -> node() end),
    ekka_autoheal:handle_msg({report_partition, node()}, ekka_autoheal:init()).

t_handle_create_splitview_msg(_) ->
    ok = meck:expect(ekka_membership, is_all_alive, fun() -> true end),
    ekka_autoheal:handle_msg({create_splitview, node()}, ekka_autoheal:init()).

t_handle_heal_partition_msg(_) ->
    ekka_autoheal:handle_msg({heal_partition, [node()]}, ekka_autoheal:init()).

