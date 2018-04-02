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

-module(ekka_cluster_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/2, stop_child/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SUP, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

start_child(M, Args) ->
    supervisor:start_child(?SUP, child_spec(M, Args)).

child_spec(M, Args) ->
    {M, {M, start_link, Args}, permanent, 5000, worker, [M]}.

stop_child(M) ->
    case supervisor:terminate_child(?SUP, M) of
        ok -> supervisor:delete_child(?SUP, M);
        {error, not_found} -> ok;
        Error -> Error
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

