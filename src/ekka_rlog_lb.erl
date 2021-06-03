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

%% @doc This server runs on the replicant and periodically checks the
%% status of core nodes in case we need to RPC to one of them.
-module(ekka_rlog_lb).

-behaviour(gen_server).

%% API
-export([ start_link/0
        ]).

%% gen_server callbacks
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% Internal exports
-export([ core_node_weight/1
        ]).

%%================================================================================
%% Type declarations
%%================================================================================

-record(s, {}).

-define(update, update).
-define(SERVER, ?MODULE).

%%================================================================================
%% API
%%================================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init(_) ->
    logger:set_process_metadata(#{domain => [ekka, rlog, lb]}),
    self() ! ?update,
    {ok, #s{}}.

handle_info(?update, St) ->
    do_update(),
    {noreply, St};
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call(_From, Call, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

do_update() ->
    [do_update(Shard) || Shard <- ekka_rlog_config:shards()],
    Interval = application:get_env(ekka, rlog_lb_update_interval, 1000),
    erlang:send_after(Interval, self(), ?update).

do_update(Shard) ->
    Timeout = application:get_env(ekka, rlog_lb_update_timeout, 300),
    CoreNodes = ekka_rlog:core_nodes(),
    case rpc:multicall(CoreNodes, ?MODULE, core_node_weight, [Shard], Timeout) of
        {[], _} ->
            ekka_rlog_status:notify_core_node_down(Shard);
        {L0, _} ->
            [{_Load, _Rand, Core}|_] = lists:sort(L0),
            ekka_rlog_status:notify_core_node_up(Shard, Core)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%% This function runs on the core node. TODO: check OLP
core_node_weight(Shard) ->
    case whereis(Shard) of
        undefined ->
            throw(shard_down);
        _Pid ->
            Load = 0,
            %% The return values will be lexicographically sorted. Load will
            %% be distributed evenly between the nodes with the same weight
            %% due to random term:
            {Load, rand:uniform(), node()}
    end.
