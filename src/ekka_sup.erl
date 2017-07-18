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

-define(CHILD(M), {M, {M, start_link, []}, permanent, 5000, worker, [M]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Membership = ?CHILD(ekka_membership),
    NodeSup = {ekka_node_sup, {ekka_node_sup, start_link, []},
               permanent, 5000, supervisor, [ekka_node_sup]},
    NodeMon = {ekka_node_monitor, {ekka_node_monitor, start_link, []},
               permanent, 5000, worker, [ekka_node_monitor]},
    ClusterSup = {ekka_cluster_sup, {ekka_cluster_sup, start_link, []},
                 permanent, infinity, supervisor, [ekka_cluster_sup]},
    {ok, {{one_for_all, 10, 100}, [Membership, NodeSup, NodeMon, ClusterSup]}}.

