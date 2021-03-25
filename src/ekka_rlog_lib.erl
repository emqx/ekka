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

%% Internal functions
-module(ekka_rlog_lib).

-export([ make_key/0
        , make_key_in_past/1
        , import_batch/2
        , rpc_call/4
        , rpc_cast/4
        , load_shard_config/0
        , read_shard_config/0
        , make_shard_match_spec/1
        %% , local_rpc_call/4
        ]).

-export_type([ tlog_entry/0
             , subscriber/0
             , table/0
             , change_type/0
             , op/0
             , tx/0
             , txid/0
             , ts/0
             , node_id/0
             , rlog/0
             ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("ekka_rlog.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type ts() :: integer().
-type node_id() :: integer().
-type txid() :: {ts(), node_id()}.

-type table():: atom().

-type change_type() :: write | delete | delete_object.

-type op() :: {{table(), term()}, term(), change_type()}
            | {ekka_rlog:func(_Ret), _Args :: list(), apply}.

-type tx() :: [op()].

-type rlog() :: #rlog{}.

-type tlog_entry() :: { _Sender :: pid()
                      , _SeqNo  :: integer()
                      , _Key    :: txid()
                      , _Tx     :: [tx()]
                      }.

-type subscriber() :: {node(), pid()}.

%%================================================================================
%% RLOG key creation
%%================================================================================

%% Log key should be globally unique.
%%
%% it is a tuple of a timestamp (ts) and the node id (node_id),
%% where ts is at nanosecond precesion to ensure it is locally
%% monotoic and unique, and node_id, which identifies the node which
%% initiated the transaction, should ensure global uniqueness.
-spec make_key() -> ekka_rlog_lib:txid().
make_key() ->
    {erlang:system_time(nanosecond), ekka_rlog:node_id()}.

-spec make_key_in_past(integer()) -> ekka_rlog_lib:txid().
make_key_in_past(Dt) ->
    {TS, Node} = make_key(),
    {TS - Dt, Node}.

%%================================================================================
%% Transaction import
%%================================================================================

%% @doc Import transaction ops to the local database
-spec import_batch(transaction | dirty, [tx()]) -> ok.
import_batch(ImportType, Batch) ->
    lists:foreach(fun(Tx) -> import_transaction(ImportType, Tx) end, Batch).

%% %% @doc Do a local RPC call, used for testing
%% -spec local_rpc_call(node(), module(), atom(), list()) -> term().
%% local_rpc_call(Node, Module, Function, Args) ->
%%     Node = node(), % assert
%%     apply(Module, Function, Args).

-spec import_transaction(transaction | dirty, [tx()]) -> ok.
import_transaction(transaction, Ops) ->
    ?tp(rlog_import_trans,
        #{ type => transaction
         , ops  => Ops
         }),
    {atomic, ok} = mnesia:transaction(
                     fun() ->
                             lists:foreach(fun import_op/1, Ops)
                     end);
import_transaction(dirty, Ops) ->
    ?tp(rlog_import_trans,
        #{ type => dirty
         , ops  => Ops
         }),
    lists:foreach(fun import_op_dirty/1, Ops).

-spec import_op(op()) -> ok.
import_op(Op) ->
    case Op of
        {{Tab, _K}, Record, write} ->
            mnesia:write(Tab, Record, write);
        {{Tab, K}, _Record, delete} ->
            mnesia:delete({Tab, K});
        {{Tab, _K}, Record, delete_object} ->
            mnesia:delete_object(Tab, Record, write)
    end.

-spec import_op_dirty(op()) -> ok.
import_op_dirty({{Tab, _K}, Record, write}) ->
    mnesia:dirty_write(Tab, Record).

%%================================================================================
%% RPC
%%================================================================================

%% @doc Do an RPC call
-spec rpc_call(node(), module(), atom(), list()) -> term().
rpc_call(Node, Module, Function, Args) ->
    Mod = persistent_term:get({ekka, rlog_rpc_mod}, gen_rpc),
    apply(Mod, call, [Node, Module, Function, Args]).

%% @doc Do an RPC cast
-spec rpc_cast(node(), module(), atom(), list()) -> term().
rpc_cast(Node, Module, Function, Args) ->
    Mod = persistent_term:get({ekka, rlog_rpc_mod}, gen_rpc),
    apply(Mod, cast, [Node, Module, Function, Args]).

%%================================================================================
%% Shard configuration
%%================================================================================

-spec load_shard_config() -> ok.
load_shard_config() ->
    Raw = read_shard_config(),
    Shards = proplists:get_keys(Raw),
    ok = persistent_term:put({ekka, shards}, Shards),
    lists:foreach(fun({Shard, Tables}) ->
                          Config = #{ tables => Tables
                                    , match_spec => make_shard_match_spec(Tables)
                                    },
                          ?tp(notice, "Setting RLOG shard config",
                              #{ shard => Shard
                               , tables => Tables
                               }),
                          ok = persistent_term:put({ekka_shard, Shard}, Config)
                  end,
                  Raw).

-spec read_shard_config() -> [{ekka_rlog:shard(), [table()]}].
read_shard_config() ->
    L = lists:flatmap( fun({_App, _Module, Attrs}) ->
                               Attrs
                       end
                     , ekka_boot:all_module_attributes(rlog_shard)
                     ),
    Shards = proplists:get_keys(L),
    [{Shard, lists:usort(proplists:get_all_values(Shard, L))}
     || Shard <- Shards].

-spec make_shard_match_spec([ekka_rlog_lib:table()]) -> ets:match_spec().
make_shard_match_spec(Tables) ->
    [{ {{Table, '_'}, '_', '_'}
     , []
     , ['$_']
     } || Table <- Tables].
