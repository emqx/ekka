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
        %% , local_rpc_call/4
        ]).

-export_type([ batch/0
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

-include("ekka_rlog.hrl").

-type ts() :: integer().
-type node_id() :: integer().
-type txid() :: {ts(), node_id()}.

-type table():: atom().

-type change_type() :: write | delete | delete_object.

-type op() :: {{table(), term()}, term(), change_type()}
            | {ekka_tx:func(), ekka_tx:args(), apply}.

-type tx() :: [op()].

-type rlog() :: #rlog{}.

-type batch() :: { _Sender :: pid()
                 , _SeqNo  :: integer()
                 , _Tx     :: [tx()]
                 }.

-type subscriber() :: {node(), pid()}.

%% Log key should be globally unique.
%%
%% it is a tuple of a timestamp (ts) and the node id (node_id),
%% where ts is at nanosecond precesion to ensure it is locally
%% monotoic and unique, and node_id, which identifies the node which
%% initiated the transaction, should ensure global uniqueness.
-spec make_key() -> ekka_rlog:txid().
make_key() ->
    {erlang:system_time(nanosecond), ekka_rlog:node_id()}.

-spec make_key_in_past(integer()) -> ekka_rlog:txid().
make_key_in_past(Dt) ->
    {TS, Node} = make_key(),
    {TS - Dt, Node}.

%% @doc Import transaction ops to the local database
-spec import_batch(transaction | dirty, [tx()]) -> ok.
import_batch(_ImportType, Batch) ->
    ok. %% TODO

%% @doc Do an RPC call
-spec rpc_call(node(), module(), atom(), list()) -> term().
rpc_call(Node, Module, Function, Args) ->
    Mod = persistent_term:get(ekka_rlog_rpc_mod, gen_rpc),
    apply(Mod, call, [Node, Module, Function, Args]).

%% @doc Do an RPC cast
-spec rpc_cast(node(), module(), atom(), list()) -> term().
rpc_cast(Node, Module, Function, Args) ->
    Mod = persistent_term:get(ekka_rlog_rpc_mod, gen_rpc),
    apply(Mod, cast, [Node, Module, Function, Args]).

%% %% @doc Do a local RPC call, used for testing
%% -spec local_rpc_call(node(), module(), atom(), list()) -> term().
%% local_rpc_call(Node, Module, Function, Args) ->
%%     Node = node(), % assert
%%     apply(Module, Function, Args).
