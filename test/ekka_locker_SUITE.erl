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

-module(ekka_locker_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [{group, locker}].

groups() ->
    [{locker, [], [aquire_local]}].

aquire_local(_Conf) ->
    Node = node(),
    {ok, Locker} = ekka_locker:start_link(test_locker),
    ?assertEqual({true, [Node]}, ekka_locker:aquire(test_locker, resource1)),
    ?assertEqual({true, [Node]}, ekka_locker:aquire(test_locker, resource1)),
    ?assertEqual({true, [Node]}, ekka_locker:release(test_locker, resource1)),
    ?assertEqual({false, [Node]}, ekka_locker:release(test_locker, resource1)),
    ekka_locker:stop(Locker).

