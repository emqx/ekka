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

%% API and management functions for asynchronous Mnesia replication
-module(ekka_rlog).

-export([ shards/0
        , core_nodes/0
        , role/0
        , node_id/0
        , subscribe_tlog/4
        ]).

-export_type([ shard/0
             , txid/0
             , ts/0
             , node_id/0
             ]).

-type shard() :: atom().

-type ts() :: integer().
-type node_id() :: integer().
-type txid() :: {ts(), node_id()}.

%% TODO: configurable
node_id() ->
    0.

%% TODO: persistent term
-spec shards() -> [shard()].
shards() ->
    application:get_env(ekka, shards, [shard1]).

%% TODO: persistent term
-spec role() -> core | replicant.
role() ->
    application:get_env(ekka, node_role, core).

-spec core_nodes() -> [node()].
core_nodes() ->
    application:get_env(ekka, core_nodes, []).

-spec subscribe_tlog(ekka_rlog:shard(), node(), pid(), ekka_rlog_server:checkpoint()) ->
          {_NeedBootstrap :: boolean(), _Agent :: pid()}
        | {badrpc | badtcp, term()}.
subscribe_tlog(Shard, RemoteNode, Subscriber, Checkpoint) ->
    MyNode = node(),
    Args = [Shard, {MyNode, Subscriber}, Checkpoint],
    ekka_rlog_lib:rpc_call(RemoteNode, ekka_rlog_server, subscribe, Args).
