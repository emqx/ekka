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

%% @doc This module implements a gen_server which periodically cleans
%% up an rlog shard to keep a cap on resource consumption.
%% Currently supported configs are:
%% capacity_bytes: number of bytes limit
%% retention_seconds: number of seconds logs to retain
%% Old logs are deleted when BOTH limits are exceeded.

-module(ekka_rlog_cleaner).

-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% default configs
-define(RETENTION_SECONDS, 600).
-define(CAPACITY_BYTERS, (1024 * 1024 * 1024)).
-define(CLEANUP_INTERCAL_SECONDS, 180).
-define(undef, undefined).

start_link(Shard) ->
    Config = #{}, %% TODO: Shard config
    gen_server:start_link(?MODULE, {Shard, Config}, []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

init({Shard, Config}) ->
    Capacity = maps:get(capacity_bytes, Config, ?CAPACITY_BYTERS),
    Retention = maps:get(retention_seconds, Config, ?RETENTION_SECONDS),
    IntervalSec = maps:get(cleanup_interval_seconds, Config, ?CLEANUP_INTERCAL_SECONDS),
    Interval = timer:seconds(IntervalSec),
    process_flag(trap_exit, true),
    _ = self() ! cleanup, %% start an immediate cleanup
    {ok, #{ shard => Shard
          , capacity_bytes => Capacity
          , retention_seconds => Retention
          , interval => Interval
          , cleanup_tref => ?undef
          }}.

handle_info(cleanup, St) ->
    NewSt = cancel_cleanup_timer(St),
    ok = cleanup(NewSt),
    {noreply, ensure_cleanup_timer(NewSt)};
handle_info(_Info, St) ->
    {noreply, St}.

handle_cast(_Cast, St) ->
    {noreply, St}.

handle_call(_From, stop, St) ->
    {stop, normal, ok, St};
handle_call(_From, Call, St) ->
    {reply, {error, {unknown_call, Call}}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, St) ->
    {ok, cancel_cleanup_timer(St)}.

ensure_cleanup_timer(#{ cleanup_tref := ?undef
                      , interval := Interval
                      } = St) ->
    St#{cleanup_tref := erlang:send_after(Interval, self(), cleanup)}.

cancel_cleanup_timer(#{cleanup_tref := Tref} = St) ->
    case is_reference(Tref) of
        true ->
            _ = erlang:cancel_timer(Tref),
            ok;
        false ->
            ok
    end,
    St#{cleanup_tref := ?undef}.

%% TODO clean up old logs
cleanup(_) -> ok.
