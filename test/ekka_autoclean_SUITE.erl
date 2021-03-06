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

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_name, ekka),
    ok = application:set_env(ekka, cluster_autoclean, 1000),
    ok = application:set_env(ekka, cluster_discovery, {manual, []}),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    ekka_ct:cleanup(?MODULE).

t_autoclean(_) ->
    N0 = node(),
    N1 = ekka_ct:start_slave(ekka, n1),
    ok = rpc:call(N1, ekka_cluster, join, [N0]),
    [N0, N1] = ekka_cluster:info(running_nodes),
    ok = ekka_ct:stop_slave(N1),
    ok = timer:sleep(2000),
    [N0] = ekka_cluster:info(running_nodes),
    ekka:force_leave(N1).
