%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Cluster via Mnesia database.
-module(ekka_cluster).

-export([info/0, info/1]).

%% Cluster API
-export([ join/1
        , leave/0
        , force_leave/1
        , status/1
        ]).

-type(info_key() :: running_nodes | stopped_nodes).

-type(infos() :: #{running_nodes := list(node()),
                   stopped_nodes := list(node())
                  }).

-export_type([info_key/0, infos/0]).

-spec(info(atom()) -> list(node())).
info(Key) -> maps:get(Key, info()).

-spec(info() -> infos()).
info() -> mria_mnesia:cluster_info().

%% @doc Cluster status of the node.
status(Node) ->
    mria_mnesia:cluster_status(Node).

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) ->
    mria:join(Node).

%% @doc Leave from the cluster.
-spec(leave() -> ok | {error, any()}).
leave() ->
    mria:leave().

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) ->
    mria:force_leave(Node).
