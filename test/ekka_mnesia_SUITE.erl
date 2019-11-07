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

-record(test_table, {key, val}).

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = ekka_mnesia:start(),
    Config.

end_per_suite(_Config) ->
    ekka_mnesia:ensure_stopped().

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_data_dir(_) ->
    ekka_mnesia:data_dir().

t_create_table(_) ->
    ok = ekka_mnesia:create_table(test_table, [
            {type, set}, {disc_copies, [node()]},
            {record_name, test_table},
            {attributes, record_info(fields, test_table)}]),
    ok = ekka_mnesia:copy_table(test_table, disc_copies).

t_copy_schema(_) ->
    ok = ekka_mnesia:copy_schema(node()).

t_delete_schema(_) ->
    ok = ekka_mnesia:delete_schema().

t_del_schema_copy(_) ->
    ok = ekka_mnesia:del_schema_copy(node()).

t_cluster_view(_) ->
    [] = ekka_mnesia:cluster_view().

t_connect(_) ->
    %% -spec(connect(node()) -> ok | {error, any()}).
    ok.

t_cluster_status(_) ->
    %% -spec(cluster_status(node()) -> running | stopped | false).
    ok.

t_remove_from_cluster(_) ->
    %% -spec(remove_from_cluster(node()) -> ok | {error, any()}).
    ok.

t_leave_cluster(_) ->
    %% -spec(leave_cluster(node()) -> ok | {error, any()}).
    ok.

t_running_nodes(_) ->
    %% -spec(running_nodes() -> list(node())).
    error('TODO').

t_join_cluster(_) ->
    %% -spec(join_cluster(node()) -> ok).
    error('TODO').

t_is_node_in_cluster(_) ->
    %% -spec(is_node_in_cluster(node()) -> boolean()).
    error('TODO').

t_cluster_nodes(_) ->
    %% -spec(cluster_nodes(all | running | stopped) -> [node()]).
    error('TODO').

