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

-module(ekka_dist_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(inet_tcp_dist, [unstick, non_strict, passthrough, no_history]),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(inet_tcp_dist),
    Config.

t_listen(_) ->
    ok = meck:expect(inet_tcp_dist, listen, fun(_Name) ->
                                                    gen_tcp:listen(0, [])
                                            end),
    {ok, Sock} = ekka_dist:listen('n@127.0.0.1'),
    ?assert(is_port(Sock)).

t_select(_) ->
    ok = meck:expect(inet_tcp_dist, select, fun(_) -> true end),
    ?assert(ekka_dist:select('n@127.0.0.1')).

t_accept(_) ->
    Self = self(),
    ok = meck:expect(inet_tcp_dist, accept, fun(_) -> Self end),
    {ok, LSock} = gen_tcp:listen(4370, []),
    Self = ekka_dist:accept(LSock),
    ok = gen_tcp:close(LSock).

t_accept_connection(_) -> ok.

t_setup(_) -> ok.

t_close(_) -> ok.

t_childspecs(_) -> ok.

t_port(_) ->
    ?assertEqual(4370, ekka_dist:port('ekka@127.0.0.1')),
    lists:foreach(
      fun(I) ->
              Node = list_to_atom("ekka" ++ integer_to_list(I) ++ "@127.0.0.1"),
              ?assertEqual(4370+I, ekka_dist:port(Node))
      end, [1,2,3]).

