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

%% @doc This module implements both bootstrap server and client

-module(ekka_rlog_bootstrapper).

%% API:
-export([start_link/2, start_link_client/3]).

%% gen_server callbacks:
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% Internal exports:
-export([do_push_batch/2]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type batch() :: { _Last :: boolean()
                 , _From :: pid()
                 , _TXs  :: [ekka_rlog_lib:tx()]
                 }.

-record(server,
        { shard       :: ekka_rlog:shard()
        , subscriber  :: ekka_rlog_lib:subscriber()
        }).

-record(client,
        { shard       :: ekka_rlog:shard()
        , server      :: pid()
        , parent      :: pid()
        }).

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Start bootstrapper server
-spec start_link(ekka_rlog:shard(), ekka_rlog_lib:subscriber()) -> {ok, pid()}.
start_link(Shard, Subscriber) ->
    gen_server:start_link(?MODULE, {server, Shard, Subscriber}, []).

%% @doc Start bootstrapper client
-spec start_link_client(ekka_rlog:shard(), node(), pid()) -> {ok, pid()}.
start_link_client(Shard, RemoteNode, Parent) ->
    gen_server:start_link(?MODULE, {client, Shard, RemoteNode, Parent}, []).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init({server, Shard, Subscriber}) ->
    %% TODO: wrong
    self() ! hack,
    {ok, #server{ shard      = Shard
                , subscriber = Subscriber
                }};
init({client, Shard, RemoteNode, Parent}) ->
    {ok, Pid} = ekka_rlog_server:bootstrap_me(RemoteNode, Shard),
    {ok, #client{ parent     = Parent
                , shard      = Shard
                , server     = Pid
                }}.

handle_info(hack, St = #server{subscriber = Subscriber}) ->
    %% TODO: don't do this.
    ok = push_batch(Subscriber, {true, self(), []}),
    {stop, normal, St};
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call({batch, {Last, Server, TXs}}, From, St = #client{server = Server, parent = Parent}) ->
    ok = ekka_rlog_lib:import_batch(dirty, TXs),
    if Last ->
            Parent ! {bootstrap_complete, self(), ekka_rlog_lib:make_key()},
            gen_server:reply(From, ok),
            {stop, normal, St};
       true ->
            {reply, ok, St}
    end;
handle_call(Call, _From, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec push_batch(ekka_rlog_lib:subscriber(), batch()) -> ok.
push_batch({Node, Pid}, Batch = {_, _, _}) ->
    ekka_rlog_lib:rpc_call(Node, ?MODULE, do_push_batch, [Pid, Batch]).

%%================================================================================
%% Internal exports (gen_rpc)
%%================================================================================

-spec do_push_batch(pid(), batch()) -> ok.
do_push_batch(Pid, Batch) ->
    gen_server:call(Pid, {batch, Batch}, infinity).
