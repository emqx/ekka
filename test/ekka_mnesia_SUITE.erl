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

-module(ekka_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_data_dir(_) ->
    error('TODO').

t_create_table(_) ->
    error('TODO').

t_copy_table(_) ->
    error('TODO').

t_copy_schema(_) ->
    error('TODO').

t_delete_schema(_) ->
    error('TODO').

t_del_schema_copy(_) ->
    error('TODO').

t_cluster_view(_) ->
    error('TODO').

t_connect(_) ->
    error('TODO').

t_cluster_status(_) ->
    error('TODO').

t_ensure_stopped(_) ->
    error('TODO').

t_remove_from_cluster(_) ->
    error('TODO').

t_leave_cluster(_) ->
    error('TODO').

t_running_nodes(_) ->
    error('TODO').

t_join_cluster(_) ->
    error('TODO').

t_is_node_in_cluster(_) ->
    error('TODO').

t_cluster_nodes(_) ->
    error('TODO').

t_start(_) ->
    error('TODO').

