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

-module(ekka).

-include("ekka.hrl").

%% Start/Stop
-export([start/0, stop/0]).

%% Register callback
-export([callback/1, callback/2]).

%% Env
-export([cluster/0, cookie/0, strategy/0, autoheal/0]).

%% Node API
-export([is_aliving/1, is_running/2]).

%% Cluster API
-export([join/1, leave/0, force_leave/1]).

%% Membership
-export([local_member/0, members/0, status/0]).

%% Monitor membership events
-export([monitor/1, unmonitor/1]).

%%%-------------------------------------------------------------------
%%% Start/Stop
%%%-------------------------------------------------------------------

start() ->
    ekka_mnesia:start(),
    application:ensure_all_started(ekka).

stop() ->
    application:stop(ekka).

%%%-------------------------------------------------------------------
%%% Register Callback
%%%-------------------------------------------------------------------

callback(Name) ->
    application:get_env(ekka, {callback, Name}).

callback(Name, Fun) ->
    application:set_env(ekka, {callback, Name}, Fun).

%%%-------------------------------------------------------------------
%%% Env
%%%-------------------------------------------------------------------

%% @doc Cluster name.
-spec(cluster() -> atom()).
cluster() -> env(cluster_name, ekka).

%% @doc Cluster cookie.
-spec(cookie() -> atom()).
cookie() -> erlang:get_cookie().

%% @doc Cluster discovery.
-spec(strategy() -> atom()).
strategy() -> env(cluster_strategy, epmd).

%% @doc Cluster autoheal.
-spec(autoheal() -> boolean()).
autoheal() -> env(cluster_autoheal, true).

env(Key, Default) ->
    application:get_env(ekka, Key, Default).

%%%-------------------------------------------------------------------
%%% Membership
%%%-------------------------------------------------------------------

%% Cluster members
-spec(members() -> list(member())).
members() -> ekka_membership:members().

%% Local member
-spec(local_member() -> member()).
local_member() -> ekka_membership:local_member().

%% Status of the cluster
status() ->
    [{members, members()}, {partitions, ekka_node_monitor:partitions()}].

%%%-------------------------------------------------------------------
%%% Node API
%%%-------------------------------------------------------------------

%% @doc Is node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) ->
    ekka_node:is_aliving(Node).

%% @doc Is the application running?
-spec(is_running(node(), atom()) -> boolean()).
is_running(Node, App) ->
    ekka_node:is_running(Node, App).

%%%-------------------------------------------------------------------
%%% Cluster API
%%%-------------------------------------------------------------------

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, any()}).
join(Node) ->
    ekka_cluster:join(Node).

%% @doc Leave from Cluster.
-spec(leave() -> ok | {error, any()}).
leave() ->
    ekka_cluster:leave().

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, any()}).
force_leave(Node) ->
    ekka_cluster:force_leave(Node).

%%%-------------------------------------------------------------------
%%% Monitor membership events
%%%-------------------------------------------------------------------

%%TODO:

monitor(membership) ->
    ekka_membership:monitor(true).

unmonitor(membership) ->
    ekka_membership:monitor(false).

