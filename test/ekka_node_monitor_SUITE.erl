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

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    {ok, _} = ekka_node_monitor:start_link(),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = ekka_node_monitor:stop(),
    Config.

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
    {ok, _TRef} = ekka_node_monitor:run_after(100, heartbeat).

t_partitions(_) ->
    [] = ekka_node_monitor:partitions().

t_handle_unexpected(_) ->
    {reply, ignore, state} = ekka_node_monitor:handle_call(req, from, state),
    {noreply, state} = ekka_node_monitor:handle_cast(msg, state),
    {noreply, state} = ekka_node_monitor:handle_info(info, state).

