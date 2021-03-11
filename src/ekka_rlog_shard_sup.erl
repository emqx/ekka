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

%% Supervision tree for the shard.
%% Runs on core nodes under `ekka_rlog_core_sup'
-module(ekka_rlog_shard_sup).

-behaviour(supervisor).

-export([init/1, start_link/1, start_link_agent_sup/1, start_link_bootstrapper_sup/1]).

%%================================================================================
%% API funcions
%%================================================================================

start_link(Shard) ->
    supervisor:start_link(?MODULE, [shard, Shard]).

start_link_agent_sup(Shard) ->
    supervisor:start_link(?MODULE, [agent, Shard]).

start_link_bootstrapper_sup(Shard) ->
    supervisor:start_link(?MODULE, [bootstrapper, Shard]).

%%================================================================================
%% Supervisor callbacks
%%================================================================================

init([shard, Shard]) ->
    SupFlags = #{ strategy => one_for_all
                , intensity => 0
                , period => 1
                },
    Children = [ server(ekka_rlog_server, Shard)
               , server(ekka_rlog_cleaner, Shard)
               ],
    {ok, {SupFlags, Children}};
init([agent, Shard]) ->
    simple_sup(ekka_rlog_server, Shard);
init([bootstrapper, Shard]) ->
    simple_sup(ekka_rlog_bootstrapper, Shard).

%%================================================================================
%% Internal functions
%%================================================================================

server(Module, Shard) ->
    #{ id => Module
     , start => {Module, start_link, [Shard]}
     , restart => permanent
     , shutdown => 1000
     , type => worker
     }.

simple_sup(Module, Shard) ->
    SupFlags = #{ strategy => simple_one_for_one
                , intensity => 0
                , period => 1
                },
    ChildSpec = #{ id => ignore
                 , start => {Module, start_link, [Shard]}
                 , restart => temporary
                 , type => worker
                 },
    {ok, {SupFlags, [ChildSpec]}}.
