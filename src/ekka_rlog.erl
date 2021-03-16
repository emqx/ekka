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

-export([ transaction/2
        , shards/0
        , core_nodes/0
        , role/0
        , node_id/0
        , subscribe/4
        ]).

-export_type([ shard/0
             ]).

-type shard() :: atom().

-type func(A) :: fun((...) -> A).

-include("ekka_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% @doc Perform a transaction and log changes.
%% the logged changes are to be replicated to other nodes.
-spec transaction(func(A), [term()]) -> mnesia:t_result(A).
transaction(F, Args) -> do(transaction, F, Args).

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

-spec subscribe(ekka_rlog:shard(), node(), pid(), ekka_rlog_server:checkpoint()) ->
          {ok, _NeedBootstrap :: boolean(), _Agent :: pid()}
        | {badrpc | badtcp, term()}.
subscribe(Shard, RemoteNode, Subscriber, Checkpoint) ->
    MyNode = node(),
    Args = [Shard, {MyNode, Subscriber}, Checkpoint],
    ekka_rlog_lib:rpc_call(RemoteNode, ekka_rlog_server, subscribe, Args).


do(Type, F, Args) ->
    Shards = ekka_rlog:shards(),
    TxFun =
        fun() ->
                Result = apply(F, Args),
                Key = ekka_rlog_lib:make_key(),
                [dig_ops_for_shard(Key, Shard) || Shard <- Shards],
                Result
        end,
    case Type of
        transaction -> mnesia:transaction(TxFun);
        async_dirty -> mnesia:async_dirty(TxFun)
    end.

get_tx_ops(F, Args) ->
    {_, _, Store} = mnesia:get_activity_id(),
    case Store of
        non_transaction ->
            args_as_op(F, Args);
        #tidstore{store = Ets} ->
            %% TODO This is probably wrong. Mnesia stores ops in ets?
            AllOps = ets:tab2list(Ets)
    end.

%% TODO: Implement proper filtering
dig_ops_for_shard(Key, Shard) ->
    case mnesia:get_activity_id() of
      {_, _, #tidstore{store = Ets}} ->
        %% TODO: get filter from config
        MS = ets:fun2ms(fun(A = {{T, _}, _, _}) when T =:= test_tab;
                                                     T =:= foobar ->
                                A
                        end),
        Ops = ets:select(Ets, MS),
        mnesia:write(Shard, #rlog{key = Key, ops = Ops}, write)
    end.

%% we can only hope that this is not an anonymous function
%% add the function is idempotent.
args_as_op(F, Args) -> [{F, Args, apply}].
