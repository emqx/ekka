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

-module(ekka_ring_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("ekka.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(ekka_membership, [non_strict, passthrough, no_history]),
    ok = meck:expect(ekka_membership, members, fun(up) -> create_nodes(5) end),
    Config.

create_nodes(Num) ->
    lists:map(fun(I) ->
                      Node = list_to_atom("node" ++ integer_to_list(I)),
                      Hash = erlang:phash2(Node, trunc(math:pow(2, 32) - 1)),
                      #member{node = Node, hash = Hash}
              end, lists:seq(1, Num)).

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(ekka_membership),
    Config.

t_find_node(_) ->
    ekka_ring:find_node(key, []).
    %%io:format("Ring: ~p~n", [ekka_ring:ring()]).

t_find_nodes(_) ->
    ekka_ring:find_nodes(key, 3, []).
    %%io:format("Ring: ~p~n", [ekka_ring:ring()]).

