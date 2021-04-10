%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Supervision tree for the core node
-module(ekka_rlog_sup).

-behaviour(supervisor).

-export([init/1, start_link/0, find_shard/1]).

-define(SUPERVISOR, ?MODULE).

-include("ekka_rlog.hrl").

%%================================================================================
%% API funcions
%%================================================================================

start_link() ->
    Shards = ekka_rlog:shards(),
    Role = ekka_rlog:role(),
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, [Role, Shards]).

-spec find_shard(ekka_rlog:shard()) -> {ok, pid()} | undefined.
find_shard(Shard) ->
    Children = [Child || {Id, Child, _, _} <- supervisor:which_children(?SUPERVISOR), Id =:= Shard],
    case Children of
        [Pid] when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            undefined
    end.

%%================================================================================
%% supervisor callbacks
%%================================================================================

init([core, Shards]) ->
    %% Shards should be restarted individually to avoid bootstrapping
    %% of too many replicants simulataneously, hence `one_for_one':
    SupFlags = #{ strategy => one_for_one
                , intensity => 100
                , period => 1
                },
    Children = [event_mgr()|lists:map(fun shard_sup/1, Shards)],
    {ok, {SupFlags, Children}};
init([replicant, Shards]) ->
    %% Create a table that holds state of the replicas:
    ets:new(?replica_tab, [ ordered_set
                          , named_table
                          , public
                          , {write_concurrency, false}
                          , {read_concurrency, true}
                          ]),
    SupFlags = #{ strategy => one_for_one
                , intensity => 100
                , period => 1
                },
    Children = [event_mgr()|lists:map(fun replicant_worker/1, Shards)],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

shard_sup(Shard) ->
    #{ id => Shard
     , start => {ekka_rlog_shard_sup, start_link, [Shard]}
     , restart => permanent
     , shutdown => 5000
     , type => supervisor
     }.

replicant_worker(Shard) ->
    #{ id => Shard
     , start => {ekka_rlog_replica, start_link, [Shard]}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.

event_mgr() ->
    #{ id => ekka_rlog_event_mgr
     , start => {ekka_rlog_event_mgr, start_link, []}
     , restart => permanent
     , shutdown => 5000
     , type => worker
     }.
