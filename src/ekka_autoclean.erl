%%%===================================================================
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(ekka_autoclean).

-include("ekka.hrl").

-export([init/0, check/1]).

-record(?MODULE, {period, timer}).

init() ->
    case ekka:env(cluster_autoclean) of
        undefined -> undefined;
        Period    -> sched(#?MODULE{period = Period})
    end.

sched(State = #?MODULE{period = Period}) ->
    State#?MODULE{timer = erlang:send_after(Period div 2, self(), autoclean)}.

check(State = #?MODULE{period = Period}) ->
    [maybe_clean(Member, Period) || Member <- ekka_membership:members(down)],
    sched(State).

maybe_clean(#member{node = Node, ltime = LTime}, Expiry) ->
    case timer:now_diff(erlang:timestamp(), LTime) > Expiry of
        true  -> ekka_mnesia:force_remove(Node);
        false -> ok
    end.

