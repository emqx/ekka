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

-module(ekka_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Delegate API
-export([start_delegate/2, delegate/1, delegates/0, stop_delegate/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

-spec(start_delegate(node(), [ekka:delegate_option()]) -> {ok, pid()} | {error, any()}).
start_delegate(Node, Options) ->
    supervisor:start_child(?SUP, delegate_spec(Node, Options)).

-spec(delegate_spec(node(), [ekka:delegate_option()]) -> supervisor:child_spec()).
delegate_spec(Node, Options) ->
    {{ekka_node, Node}, {ekka_node, delegate, [Node, Options]},
      permanent, 5000, worker, [ekka_node]}.

%% @doc List all delegates
-spec(delegates() -> [{node(), pid()}]).
delegates() ->
    [{Node, Pid} || {{ekka_node, Node}, Pid, worker, _}
                    <- supervisor:which_children(?SUP)].

-spec(delegate(node()) -> {ok, pid()} | false).
delegate(Node) ->
    case lists:keysearch(Node, 1, delegates()) of
        {value, {Node, Pid}} -> {ok, Pid};
        false -> false
    end.

%% @doc Stop a delegate
-spec(stop_delegate(node()) -> ok | {error, any()}).
stop_delegate(Node) ->
    ChildId = {ekka_node, Node},
    case supervisor:terminate_child(?SUP, ChildId) of
        ok    -> supervisor:delete_child(?SUP, ChildId);
        Error -> Error
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

