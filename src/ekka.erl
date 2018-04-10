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

-module(ekka).

-include("ekka.hrl").

%% Start/Stop, and Env
-export([start/0, env/1, env/2, stop/0]).

%% Register callback
-export([callback/1, callback/2]).

%% Autocluster
-export([autocluster/0, autocluster/2]).

%% Node API
-export([is_aliving/1, is_running/2]).

%% Cluster API
-export([cluster_name/0]).
-export([join/1, leave/0, force_leave/1]).

%% Membership
-export([nodelist/0, nodelist/1]).
-export([local_member/0, members/0, is_member/1, status/0]).

%% Monitor membership events
-export([monitor/1, unmonitor/1]).

%% Locker API
-export([lock/1, lock/2, lock/3, unlock/1, unlock/2]).

%%--------------------------------------------------------------------
%% Start/Stop
%%--------------------------------------------------------------------

-spec(start() -> ok).
start() ->
    ekka_mnesia:start(), {ok, _Apps} = application:ensure_all_started(ekka), ok.

-spec(stop() -> ok).
stop() ->
    application:stop(ekka).

%%--------------------------------------------------------------------
%% Env
%%--------------------------------------------------------------------

env(Key) ->
    application:get_env(ekka, Key).

env(Key, Default) ->
    application:get_env(ekka, Key, Default).

%%--------------------------------------------------------------------
%% Register Callback
%%--------------------------------------------------------------------

callback(Name) ->
    application:get_env(ekka, {callback, Name}).

callback(Name, Fun) ->
    application:set_env(ekka, {callback, Name}, Fun).

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------

autocluster() ->
    autocluster(ekka, fun() -> ok end).

autocluster(App, Fun) ->
    case ekka_autocluster:aquire_lock() of
        ok ->
            spawn(fun() ->
                    group_leader(whereis(init), self()),
                    wait_application_ready(App, 5),
                    try
                        ekka_autocluster:discover_and_join(Fun)
                    catch
                        _:Error -> lager:error("Autocluster exception: ~p", [Error])
                    end,
                    ekka_autocluster:release_lock()
                  end);
        failed ->
            ignore
    end.

wait_application_ready(_App, 0) ->
    timeout;
wait_application_ready(App, Retries) ->
    case ekka_node:is_running(App) of
        true  -> ok;
        false -> timer:sleep(1000),
                 wait_application_ready(App, Retries - 1)
    end.

%%--------------------------------------------------------------------
%% Membership API
%%--------------------------------------------------------------------

%% Cluster members
-spec(members() -> list(member())).
members() ->
    ekka_membership:members().

%% Local member
-spec(local_member() -> member()).
local_member() ->
    ekka_membership:local_member().

%% Is node a member?
-spec(is_member(node()) -> boolean()).
is_member(Node) ->
    ekka_membership:is_member(Node).

%% Node list
-spec(nodelist() -> list(node())).
nodelist() ->
    ekka_membership:nodelist().

nodelist(Status) ->
    ekka_membership:nodelist(Status).

%% Status of the cluster
status() ->
    [{members, members()}, {partitions, ekka_node_monitor:partitions()}].

%%--------------------------------------------------------------------
%% Node API
%%--------------------------------------------------------------------

%% @doc Is node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) ->
    ekka_node:is_aliving(Node).

%% @doc Is the application running?
-spec(is_running(node(), atom()) -> boolean()).
is_running(Node, App) ->
    ekka_node:is_running(Node, App).

%%--------------------------------------------------------------------
%% Cluster API
%%--------------------------------------------------------------------

-spec(cluster_name() -> cluster()).
cluster_name() ->
    env(cluster_name, undefined).

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

%%--------------------------------------------------------------------
%% Monitor membership events
%%--------------------------------------------------------------------

%%TODO:

monitor(membership) ->
    ekka_membership:monitor(true).

unmonitor(membership) ->
    ekka_membership:monitor(false).

%%--------------------------------------------------------------------
%% Locker API
%%--------------------------------------------------------------------

-spec(lock(ekka_locker:resource()) -> ekka_locker:lock_result()).
lock(Resource) ->
    ekka_locker:aquire(Resource).

-spec(lock(ekka_locker:resource(), ekka_locker:lock_type())
      -> ekka_locker:lock_result()).
lock(Resource, Type) ->
    ekka_locker:aquire(ekka_locker, Resource, Type).

-spec(lock(ekka_locker:resource(), ekka_locker:lock_type(), ekka_locker:piggyback())
      -> ekka_locker:lock_result()).
lock(Resource, Type, Piggyback) ->
    ekka_locker:aquire(ekka_locker, Resource, Type, Piggyback).

-spec(unlock(ekka_locker:resource()) -> boolean()).
unlock(Resource) ->
    ekka_locker:release(Resource).

-spec(unlock(ekka_locker:resource(), ekka_locker:lock_type()) -> boolean()).
unlock(Resource, Type) ->
    ekka_locker:release(ekka_locker, Resource, Type).

