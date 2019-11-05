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

-module(ekka_node_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

t_parse_name(_) ->
    'a@127.0.0.1' = ekka_node:parse_name("a@127.0.0.1"),
    'b@127.0.0.1' = ekka_node:parse_name("b").

t_is_running(_) ->
    error('TODO').

t_is_aliving(_) ->
    io:format("Node: ~p~n", [node()]),
    true = ekka_node:is_aliving(node()),
    false = ekka_node:is_aliving('x@127.0.0.1').

