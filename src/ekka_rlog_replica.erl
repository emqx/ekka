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

%% @doc This module implements a gen_statem which collects rlogs from
%% a remote core node.
-module(ekka_rlog_replica).

%% API:
-export([start_link/1]).

%% gen_statem callbacks:
-export([init/1, terminate/3, code_change/4, callback_mode/0, handle_event/4]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% States:
-define(disconnected, disconnected).
-define(bootstrap, bootstrap).
-define(local_replay, local_replay).
-define(normal, normal).

-type state() :: ?bootstrap
               | ?local_replay
               | ?normal
               | ?disconnected.

-record(d,
        { agent                        :: pid()
        , bootstrap_client = undefined :: pid() | undefined
        , checkpoint       = undefined :: ekka_rlog_server:checkpoint()
        }).

-type data() :: #d{}.

%%================================================================================
%% API funcions
%%================================================================================

start_link(Shard) ->
    Config = #{}, % TODO
    gen_statem:start_link({local, Shard}, ?MODULE, {Shard, Config}, []).

%%================================================================================
%% gen_statem callbacks
%%================================================================================

callback_mode() -> [handle_event_function, state_enter].

-spec init({ekka_rlog:shard(), any()}) -> {ok, state(), data()}.
init({Shard, _Opts}) ->
    ?tp(ekka_rlog_replica_start,
        #{ node => node()
         , shard => Shard
         }),
    {ok, ?disconnected, #d{}}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) ->
          gen_statem:event_handler_result(state()).
%% `disconnected' state:
handle_event(enter, _, ?disconnected, D) ->
    {keep_state_and_data, [{timeout, 0, reconnect}]};
handle_event(timeout, reconnect, ?disconnected, D) ->
    handle_reconnect(D);
%% `bootstrap' state:
%% `local_replay' state:
%% `normal' state:
%% Common actions:
handle_event(enter, OldState, State, Data) ->
    handle_state_trans(OldState, State, Data),
    keep_state_and_data;
handle_event(EventType, Event, State, Data) ->
    handle_unknown(EventType, Event, State, Data),
    keep_state_and_data.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, _Data) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec handle_reconnect(data()) -> gen_statem:event_handler_result(state()).
handle_reconnect(D) ->
    case try_connect(D#d.checkpoint) of
        {ok, _BootstrapNeeded = true, ConnPid} ->
            {next_state, ?bootstrap, #d{agent = ConnPid}};
        {ok, _BootstrapNeeded = false, ConnPid} ->
            {next_state, ?normal, D#d{agent = ConnPid}};
        {error, Err} ->
            ReconnectTimeout = application:get_env(ekka, rlog_replica_reconnect_interval, 5000),
            {keep_state_and_data, [{timeout, ReconnectTimeout, reconnect}]}
    end.

-spec try_connect(ekka_rlog_server:checkpoint()) -> {ok, boolean(), pid()} | {error, term()}.
try_connect(Checkpoint) ->
    {error, not_implemented}.

handle_unknown(EventType, Event, State, Data) ->
    ?tp(warning, "rlog agent received unknown event",
        #{ event_type => EventType
         , event => Event
         , state => State
         , data => Data
         }).

handle_state_trans(OldState, State, _Data) ->
    ?tp(rlog_agent_state_change,
        #{ from => OldState
         , to => State
         }).
