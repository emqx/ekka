%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_cluster_k8s_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(OPTIONS, [{apiserver, "http://10.110.111.204:8080"},
                  {namespace, "default"},
                  {service_name, "ekka"},
                  {address_type, ip},
                  {suffix, ""}
                 ]).

all() -> ekka_ct:all(?MODULE).

t_discover(_) ->
    ok = meck:new(httpc, [non_strict, passthrough, no_history]),
    Json = <<"{\"subsets\": [{\"addresses\": [{\"ip\": \"192.168.10.10\"}]}]}">>,
    ok = meck:expect(httpc, request, fun(get, _Req, _Opts, _) ->
                                             {ok, {{"HTTP/1.1", 200, "OK"}, [], Json}}
                                     end),
    {ok, ['ekka@192.168.10.10']} = ekka_cluster_strategy:discover(ekka_cluster_k8s, [{app_name, "ekka"} | ?OPTIONS]),
    %% below test relies on rebar3 ct is run with '--name ct@127.0.0.1'
    {ok, ['ct@192.168.10.10']} = ekka_cluster_strategy:discover(ekka_cluster_k8s, ?OPTIONS),
    ok = meck:unload(httpc).

t_lock(_) ->
    ignore = ekka_cluster_static:lock([]).

t_unlock(_) ->
    ignore = ekka_cluster_static:unlock([]).

t_register(_) ->
    ignore = ekka_cluster_static:register([]).

t_unregister(_) ->
    ignore = ekka_cluster_static:unregister([]).
