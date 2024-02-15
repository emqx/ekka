%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka).

-include_lib("mria/include/mria.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Start/Stop
-export([start/0, stop/0]).

%% Env
-export([env/1, env/2]).

%% Info
-export([info/0, info/1]).

%% Cluster API
-export([cluster_name/0]).
-export([ join/1
        , leave/0
        , force_leave/1
        ]).

%% Autocluster API
-export([ autocluster/0
        , autocluster/1
        ]).

%% Callbacks
-export([ exec_callback/1
        , callback/2
        ]).

%% Node API
-export([ is_aliving/1
        , is_running/2
        ]).

%% Membership API
-export([ members/0
        , is_member/1
        , local_member/0
        , nodelist/0
        , nodelist/1
        ]).

%% Membership Monitor API
-export([ monitor/1
        , monitor/2
        , unmonitor/1
        , unmonitor/2
        ]).

%% Locker API
-export([ lock/1
        , lock/2
        , lock/3
        , unlock/1
        , unlock/2
        ]).

-define(IS_MON_TYPE(T), T == membership orelse T == partition).

-type(info_key() :: members | running_nodes | stopped_nodes | partitions).

-type(infos() :: #{members       := list(member()),
                   running_nodes := list(node()),
                   stopped_nodes := list(node()),
                   partitions    := list(node())
                  }).

-export_type([info_key/0, infos/0]).

%%--------------------------------------------------------------------
%% Start/Stop
%%--------------------------------------------------------------------

-spec(start() -> ok).
start() ->
    ok = mria:start(),
    ?tp(info, "Starting ekka", #{}),
    ekka_boot:register_mria_callbacks(),
    {ok, _Apps} = application:ensure_all_started(ekka),
    ?tp(info, "Ekka is running", #{}),
    maybe_create_tables(),
    ekka:exec_callback(start),
    ok.

maybe_create_tables() ->
    case env(boot_create_tables, true) of
        true -> ekka_boot:create_tables();
        false -> ok
    end.

-spec(stop() -> ok).
stop() ->
    ekka:exec_callback(stop),
    application:stop(ekka).

%%--------------------------------------------------------------------
%% Env
%%--------------------------------------------------------------------

-spec(env(atom() | {callback, atom()}) -> undefined | {ok, term()}).
env(Key) ->
    %% TODO: hack, using apply to trick dialyzer.
    apply(application, get_env, [ekka, Key]).

-spec(env(atom() | {callback, atom()}, term()) -> term()).
env(Key, Default) ->
    application:get_env(ekka, Key, Default).

%%--------------------------------------------------------------------
%% Info
%%--------------------------------------------------------------------

-spec(info(info_key()) -> term()).
info(Key) ->
    maps:get(Key, info()).

-spec(info() -> infos()).
info() ->
    ClusterInfo = ekka_cluster:info(),
    Partitions = mria_node_monitor:partitions(),
    maps:merge(ClusterInfo, #{members    => members(),
                              partitions => Partitions
                             }).

%%--------------------------------------------------------------------
%% Cluster API
%%--------------------------------------------------------------------

-spec(cluster_name() -> cluster()).
cluster_name() -> env(cluster_name, undefined).

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) -> ekka_cluster:join(Node).

%% @doc Leave from Cluster.
-spec(leave() -> ok | {error, term()}).
leave() -> ekka_cluster:leave().

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) -> ekka_cluster:force_leave(Node).

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------

autocluster() -> autocluster(ekka).

autocluster(App) ->
    case env(cluster_enable, true) andalso ekka_autocluster:enabled() of
        true  -> ekka_autocluster:run(App);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
-spec exec_callback(atom()) -> term().
exec_callback(Name) ->
    case env({callback, Name}) of
      {ok, Fun} -> Fun();
      undefined -> ok
    end.

-spec(callback(atom(), function()) -> ok).
callback(Name, Fun) ->
    %% TODO: hack, using apply to trick dialyzer.
    %% Using a tuple as a key of the application environment "works", but it violates the spec
    apply(application, set_env, [ekka, {callback, Name}, Fun]).

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
%% Membership API
%%--------------------------------------------------------------------

%% Cluster members
-spec(members() -> list(member())).
members() -> mria_membership:members().

%% Local member
-spec(local_member() -> member()).
local_member() -> mria_membership:local_member().

%% Is node a member?
-spec(is_member(node()) -> boolean()).
is_member(Node) -> mria_membership:is_member(Node).

%% Node list
-spec(nodelist() -> list(node())).
nodelist() -> mria_membership:nodelist().

-spec(nodelist(up|down) -> list(node())).
nodelist(Status) -> mria_membership:nodelist(Status).

%%--------------------------------------------------------------------
%% Membership Monitor API
%%--------------------------------------------------------------------

monitor(Type) when ?IS_MON_TYPE(Type) ->
    mria_membership:monitor(Type, self(), true).

monitor(Type, Fun) when is_function(Fun), ?IS_MON_TYPE(Type) ->
    mria_membership:monitor(Type, Fun, true).

unmonitor(Type) when ?IS_MON_TYPE(Type) ->
    mria_membership:monitor(Type, self(), false).

unmonitor(Type, Fun) when is_function(Fun), ?IS_MON_TYPE(Type) ->
    mria_membership:monitor(Type, Fun, false).

%%--------------------------------------------------------------------
%% Locker API
%%--------------------------------------------------------------------

-spec(lock(ekka_locker:resource()) -> ekka_locker:lock_result()).
lock(Resource) ->
    ekka_locker:acquire(Resource).

-spec(lock(ekka_locker:resource(), ekka_locker:lock_type())
      -> ekka_locker:lock_result()).
lock(Resource, Type) ->
    ekka_locker:acquire(ekka_locker, Resource, Type).

-spec(lock(ekka_locker:resource(), ekka_locker:lock_type(), ekka_locker:piggyback())
      -> ekka_locker:lock_result()).
lock(Resource, Type, Piggyback) ->
    ekka_locker:acquire(ekka_locker, Resource, Type, Piggyback).

-spec(unlock(ekka_locker:resource()) -> ekka_locker:lock_result()).
unlock(Resource) ->
    ekka_locker:release(Resource).

-spec(unlock(ekka_locker:resource(), ekka_locker:lock_type()) -> ekka_locker:lock_result()).
unlock(Resource, Type) ->
    ekka_locker:release(ekka_locker, Resource, Type).
