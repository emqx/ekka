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

-export([ init/0

        , role/0
        , role/1
        , backend/0

        , transaction/2
        , ensure_shard/1
        , core_nodes/0
        , subscribe/4
        , wait_for_shards/2

        , get_internals/0
        ]).

-export_type([ shard/0
             , role/0
             , shard_config/0
             ]).

-include("ekka_rlog.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-type shard() :: atom().

-type role() :: core | replicant.

-type shard_config() :: #{ tables := [ekka_rlog_lib:table()]
                         , match_spec := ets:match_spec()
                         }.

init() ->
    ekka_rlog_config:init().

-spec role() -> ekka_rlog:role().
role() ->
    ekka_rlog_config:role().

-spec role(node()) -> ekka_rlog:role().
role(Node) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, role, []).

backend() ->
    ekka_rlog_config:backend().

%% @doc Perform a transaction and log changes.
%% the logged changes are to be replicated to other nodes.
-spec transaction(atom(), [term()]) -> ekka_mnesia:t_result(term()).
transaction(F, Args) -> do(transaction, F, Args).

-spec core_nodes() -> [node()].
core_nodes() ->
    application:get_env(ekka, core_nodes, []).

-spec ensure_shard(shard()) -> ok.
ensure_shard(Shard) ->
    case ekka_rlog_sup:start_shard(Shard) of
        {ok, _}                       -> ok;
        {error, already_present}      -> ok;
        {error, {already_started, _}} -> ok
    end.

-spec subscribe(ekka_rlog:shard(), node(), pid(), ekka_rlog_server:checkpoint()) ->
          {ok, _NeedBootstrap :: boolean(), _Agent :: pid()}
        | {badrpc | badtcp, term()}.
subscribe(Shard, RemoteNode, Subscriber, Checkpoint) ->
    MyNode = node(),
    Args = [Shard, {MyNode, Subscriber}, Checkpoint],
    ekka_rlog_lib:rpc_call(RemoteNode, ekka_rlog_server, subscribe, Args).

-spec get_internals() -> {ekka_rlog_lib:mnesia_tid(), ets:tab()}.
get_internals() ->
    case mnesia:get_activity_id() of
        {_, TID, #tidstore{store = TxStore}} ->
            {TID, TxStore}
    end.

do(Type, F, Args) ->
    case mnesia:get_activity_id() of
        undefined ->
            Shards = ekka_rlog_config:shards(),
            TxFun =
                fun() ->
                        Result = apply(ekka_rlog_activity, F, Args),
                        {TID, TxStore} = get_internals(),
                        Key = ekka_rlog_lib:make_key(TID),
                        [dig_ops_for_shard(Key, TxStore, Shard) || Shard <- Shards],
                        Result
                end,
            case Type of
                transaction -> mnesia:transaction(TxFun)
                %%  async_dirty -> mnesia:async_dirty(TxFun)
            end;
        _ ->
            error(nested_transaction)
    end.

-spec wait_for_shards([shard()], timeout()) -> ok | {timeout, [shard()]}.
wait_for_shards(Shards, Timeout) ->
    case ekka_rlog_config:backend() of
        rlog ->
            [ok = ensure_shard(I) || I <- Shards],
            case role() of
                core ->
                    ok;
                replicant ->
                    ekka_rlog_status:wait_for_shards(Shards, Timeout)
            end;
        mnesia ->
            ok
    end.

dig_ops_for_shard(Key, TxStore, Shard) ->
    #{match_spec := MS} = ekka_rlog_config:shard_config(Shard),
    Ops = ets:select(TxStore, MS),
    mnesia:write(Shard, #rlog{key = Key, ops = Ops}, write).

%% get_tx_ops(F, Args) ->
%%     {_, _, Store} = mnesia:get_activity_id(),
%%     case Store of
%%         non_transaction ->
%%             args_as_op(F, Args);
%%         #tidstore{store = Ets} ->
%%             %% TODO This is probably wrong. Mnesia stores ops in ets?
%%             AllOps = ets:tab2list(Ets)
%%     end.

%% we can only hope that this is not an anonymous function
%% add the function is idempotent.
%% args_as_op(F, Args) -> [{F, Args, apply}].
