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

-module(ekka_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ClusterSup = {ekka_cluster_sup, {ekka_cluster_sup, start_link, []},
                  permanent, infinity, supervisor, [ekka_cluster_sup]},
    Membership = {ekka_membership, {ekka_membership, start_link, []},
                  permanent, 5000, worker, [ekka_membership]},
    NodeMonitor = {ekka_node_monitor, {ekka_node_monitor, start_link, []},
                   permanent, 5000, worker, [ekka_node_monitor]},
    {ok, {{one_for_all, 10, 100}, [ClusterSup, Membership, NodeMonitor]}}.

