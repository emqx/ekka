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

%% @doc This module accepts watch and bootstrap requests, and spawns
%% workers processes.

-module(ekka_rlog_server).

-behaviour(gen_server).

%% API
-export([ start_link/1
        , subscribe_tlog/3
        , bootstrap/2
        ]).

%% gen_server callbacks
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

-export_type([ checkpoint/0
             ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type checkpoint() :: ekka_rlog:txid() | undefined.

-record(s,
        { agent_sup           :: pid()
        , bootstrapper_sup    :: pid()
        , tlog_replay         :: integer()
        , bootstrap_threshold :: integer()
        }).

%%================================================================================
%% API funcions
%%================================================================================

start_link(Shard) ->
    Config = #{}, % TODO
    gen_server:start_link({local, Shard}, ?MODULE, {Shard, Config}, []).

-spec subscribe_tlog(ekka_rlog:shard(), pid(), checkpoint()) -> ok.
subscribe_tlog(Shard, Subscriber, Checkpoint) ->
    gen_server:call(Shard, {subscribe_tlog, Subscriber, Checkpoint}, infinity).

-spec bootstrap(ekka_rlog:shard(), pid()) -> ok.
bootstrap(Shard, Subscriber) ->
    gen_server:call(Shard, {bootstrap, Subscriber}, infinity).

%%================================================================================
%% gen_server callbacks
%%================================================================================

init({Shard, Config}) ->
    ?tp(ekka_rlog_server_start,
        #{ node => node()
         , shard => Shard
         }),
    {ok, AgentSup} = ekka_rlog_shard_sup:start_link_agent_sup(Shard),
    {ok, BootstrapperSup} = ekka_rlog_shard_sup:start_link_bootstrapper_sup(Shard),
    TlogReplay =
        erlang:convert_time_unit(maps:get(tlog_replay, Config, 10), second, nanosecond),
    BootstrapThreshold =
        erlang:convert_time_unit(maps:get(tlog_replay, Config, 300), second, nanosecond),
    {ok, #s{ agent_sup           = AgentSup
           , bootstrapper_sup    = BootstrapperSup
           , tlog_replay         = TlogReplay
           , bootstrap_threshold = BootstrapThreshold
           }}.

handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call(_From, {subscribe_tlog, Subscriber, Checkpoint}, State) ->
    {NeedBootstrap, ReplaySince} = needs_bootstrap( State#s.bootstrap_threshold
                                                  , State#s.tlog_replay
                                                  , Checkpoint
                                                  ),
    Pid = maybe_start_child(State#s.agent_sup, [Subscriber, ReplaySince]),
    {reply, {NeedBootstrap, Pid}, State};
handle_call(_From, {bootstrap, Subscriber}, State) ->
    Pid = maybe_start_child(State#s.bootstrapper_sup, [Subscriber]),
    {reply, {ok, Pid}, State};
handle_call(_From, Call, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, #{} = St) ->
    {ok, St}.

%%================================================================================
%% Internal functions
%%================================================================================

%% @private Check if the remote needs to bootstrap itself
-spec needs_bootstrap(integer(), integer(), checkpoint()) -> {boolean(), integer()}.
needs_bootstrap(BootstrapThreshold, Replay, Checkpoint) ->
    {BootstrapDeadline, _} = ekka_rlog_lib:make_key_in_past(BootstrapThreshold),
    case Checkpoint of
        {TS, _Node} when TS > BootstrapDeadline ->
            {false, TS - Replay};
        _ ->
            {ReplaySince, _} = ekka_rlog_lib:make_key_in_past(Replay),
            {true, ReplaySince}
    end.

-spec maybe_start_child(pid(), list()) -> pid().
maybe_start_child(Supervisor, Args) ->
    case supervisor:start_child(Supervisor, Args) of
        {ok, Pid} -> Pid;
        {ok, Pid, _} -> Pid;
        {error, {already_started, Pid}} -> Pid
    end.
