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

-module(ekka_cluster_mcast_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(OPTIONS, [{addr, {239,192,0,1}},
                  {ports, [4369,4370]},
                  {iface, {0,0,0,0}},
                  {ttl, 255},
                  {loop, true}
                 ]).

all() -> ekka_ct:all(?MODULE).

t_discover(_) ->
    ok = meck:new(ekka_cluster_sup, [non_strict, passthrough, no_history]),
    ok = meck:expect(ekka_cluster_sup, start_child,
                     fun(_, _) ->
                             ekka_cluster_mcast:start_link(?OPTIONS)
                     end),
    ok = meck:new(gen_udp, [non_strict, passthrough, no_history]),
    ok = meck:expect(gen_udp, send, fun(_, _, _, _) -> ok end),
    %% Simulate a UDP packet.
    Cookie = erlang:phash2(erlang:get_cookie()),
    Pong = {pong, 'node1@192.168.10.10', ekka, Cookie},
    Datagram = {udp, sock, {127,0,0,1}, 5000, term_to_binary(Pong)},
    ekka_cluster_mcast ! Datagram,
    {ok, ['node1@192.168.10.10']} = ekka_cluster_mcast:discover(?OPTIONS),
    ok = ekka_cluster_mcast:stop(),
    ok = meck:unload(ekka_cluster_sup).

t_lock(_) ->
    ignore = ekka_cluster_mcast:lock([]).

t_unlock(_) ->
    ignore = ekka_cluster_mcast:unlock([]).

t_register(_) ->
    ignore = ekka_cluster_mcast:register([]).

t_unregister(_) ->
    ignore = ekka_cluster_mcast:unregister([]).

