%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(ekka_lib_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() -> [{group, node}].

groups() ->
    [{node, [], [node_is_aliving, node_parse_name]}].

%%--------------------------------------------------------------------
%% ekka_node
%%--------------------------------------------------------------------

node_is_aliving(_) ->
    io:format("Node: ~p~n", [node()]),
    true = ekka_node:is_aliving(node()),
    false = ekka_node:is_aliving('x@127.0.0.1').

node_parse_name(_) ->
    'a@127.0.0.1' = ekka_node:parse_name("a@127.0.0.1"),
    'b@127.0.0.1' = ekka_node:parse_name("b").


