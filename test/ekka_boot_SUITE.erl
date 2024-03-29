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

-module(ekka_boot_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

%% TODO:
t_apply_module_attributes(_) ->
    %% Arg is Attribute Name
    [] =  ekka_boot:apply_module_attributes(xattr).

%% TODO:
t_all_module_attributes(_) ->
    [] = ekka_boot:all_module_attributes(xattr).

t_register_mria_callbacks(_) ->
    try
        ok = ekka_boot:register_mria_callbacks(),
        ?assertEqual(
           {ok, fun ekka:start/0},
           mria_config:callback(start)),
        ?assertEqual(
           {ok, fun ekka:stop/0},
           mria_config:callback(stop)),
        ?assertEqual(
           {ok, fun ekka_autocluster:core_node_discovery_callback/0},
           mria_config:callback(core_node_discovery))
    after
        lists:foreach(
          fun mria_config:unregister_callback/1,
          [ start
          , stop
          , core_node_discovery
          ])
    end.
