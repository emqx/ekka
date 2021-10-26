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

-module(ekka_cluster_dns_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

init_per_testcase(t_discover, Config) ->
    meck:new(inet_res, [non_strict, passthrough, no_history, no_link, unstick]),
    meck:expect(
        inet_res,
        lookup,
        fun (_, _, a) -> [{127, 0, 0, 1}];
            (_, _, srv) -> [{0,6,8083,"emqx-0.emqx.default.svc.cluster.local"},
                            {0,6,8084,"emqx-0.emqx.default.svc.cluster.local"},
                            {0,6,18083,"emqx-0.emqx.default.svc.cluster.local"},
                            {0,6,8883,"emqx-0.emqx.default.svc.cluster.local"},
                            {0,6,1883,"emqx-0.emqx.default.svc.cluster.local"},
                            {0,6,8083,"emqx-1.emqx.default.svc.cluster.local"},
                            {0,6,8084,"emqx-1.emqx.default.svc.cluster.local"},
                            {0,6,18083,"emqx-1.emqx.default.svc.cluster.local"},
                            {0,6,8883,"emqx-1.emqx.default.svc.cluster.local"},
                            {0,6,1883,"emqx-1.emqx.default.svc.cluster.local"},
                            {0,6,8083,"emqx-2.emqx.default.svc.cluster.local"},
                            {0,6,8084,"emqx-2.emqx.default.svc.cluster.local"},
                            {0,6,18083,"emqx-2.emqx.default.svc.cluster.local"},
                            {0,6,8883,"emqx-2.emqx.default.svc.cluster.local"},
                            {0,6,1883,"emqx-2.emqx.default.svc.cluster.local"}
                           ];
            (Name, Class, Type) -> meck:passthrough([Name, Class, Type])
        end),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(t_discover, Config) ->
    meck:unload(inet_res),
    Config;
end_per_testcase(_, Config) -> Config.

all() -> ekka_ct:all(?MODULE).

t_discover(_) ->
    Options1 = [{name, "localhost"}, {app, "ekka"}],
    {ok, ['ekka@127.0.0.1']} = ekka_cluster_dns:discover(Options1),

    Options2 = [{name, "emqx.default.svc.cluster.local"}, {app, "ekka"}, {type, srv}],
    {ok, ['ekka@emqx-0.emqx.default.svc.cluster.local',
          'ekka@emqx-1.emqx.default.svc.cluster.local',
          'ekka@emqx-2.emqx.default.svc.cluster.local'
         ]} = ekka_cluster_dns:discover(Options2),
    ok.

t_lock(_) ->
    ignore = ekka_cluster_dns:lock([]).

t_unlock(_) ->
    ignore = ekka_cluster_dns:unlock([]).

t_register(_) ->
    ignore = ekka_cluster_dns:register([]).

t_unregister(_) ->
    ignore = ekka_cluster_dns:unregister([]).

