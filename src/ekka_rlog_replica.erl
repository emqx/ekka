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
-export([start_link/1, push_batch/3]).

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
        { shard                        :: ekka_rlog:shard()
        , remote_core_node = undefined :: node() | undefined
        , agent                        :: pid() | undefined
        , tmp_worker       = undefined :: pid() | undefined
        , checkpoint       = undefined :: ekka_rlog_server:checkpoint()
        , next_batch_seqno = 0         :: integer()
        }).

-type data() :: #d{}.

-type fsm_result() :: gen_statem:event_handler_result(state()).

%%================================================================================
%% API funcions
%%================================================================================

%% This function is called by the remote core node.
-spec push_batch(node(), ekka_rlog:shard(), ekka_rlog_lib:batch()) -> ok.
push_batch(Node, Shard, Batch) ->
    ekka_rlog_lib:rpc_call(Node, gen_statem, call, [Shard, {tlog_batch, Batch}, infinity]).

start_link(Shard) ->
    Config = #{}, % TODO
    gen_statem:start_link(?MODULE, {Shard, Config}, []).

%%================================================================================
%% gen_statem callbacks
%%================================================================================

callback_mode() -> [handle_event_function, state_enter].

-spec init({ekka_rlog:shard(), any()}) -> {ok, state(), data()}.
init({Shard, _Opts}) ->
    process_flag(trap_exit, true),
    ?tp(ekka_rlog_replica_start,
        #{ node => node()
         , shard => Shard
         }),
    D = #d{ shard = Shard
          },
    {ok, ?disconnected, D}.

-spec handle_event(gen_statem:event_type(), _EventContent, state(), data()) -> fsm_result().
handle_event(call, {tlog_batch, Batch}, State, D) ->
    handle_batch(State, Batch, D);
%% Events specific to `disconnected' state:
handle_event(enter, OldState, ?disconnected, D) ->
    handle_state_trans(OldState, ?disconnected, D),
    {keep_state_and_data, [{timeout, 0, reconnect}]};
handle_event(timeout, reconnect, ?disconnected, D) ->
    handle_reconnect(D);
%% Events specific to `bootstrap' state:
handle_event(cast, {bootstrap_complete, Pid, Checkpoint}, ?bootstrap, D = #d{tmp_worker = Pid}) ->
    handle_bootstrap_complete(Checkpoint, D);
handle_event(enter, OldState, ?bootstrap, D) ->
    handle_state_trans(OldState, ?bootstrap, D),
    initiate_bootstrap(D);
%% Events specific to `local_replay' state:
handle_event(enter, OldState, ?local_replay, D) ->
    handle_state_trans(OldState, ?local_replay, D),
    init_local_replay(D);
handle_event(call, {local_replay_complete, Worker}, ?local_replay, D = #d{tmp_worker = Worker}) ->
    complete_initialization(D);
%% Events specific to `normal' state:
%% Common events:
handle_event(enter, OldState, State, Data) ->
    handle_state_trans(OldState, State, Data);
handle_event(info, {'EXIT', Worker, Reason}, State, D = #d{tmp_worker = Worker}) ->
    handle_worker_down(State, Reason, D);
handle_event(info, {'EXIT', Agent, Reason}, State, D = #d{agent = Agent}) ->
    handle_agent_down(State, Reason, D);
handle_event(EventType, Event, State, Data) ->
    handle_unknown(EventType, Event, State, Data).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, _Data) ->
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

%% @private Consume transactions from the core node
-spec handle_batch(state(), ekka_rlog_lib:batch(), data()) -> fsm_result().
handle_batch(?normal, {Agent, SeqNo, Transactions},
             D = #d{ agent            = Agent
                   , next_batch_seqno = SeqNo
                   }) ->
    %% Normal flow, transactions are applied directly to the replica:
    ?tp(rlog_replica_import_batch,
        #{ agent => Agent
         , seqno => SeqNo
         , transactions => Transactions
         }),
    ekka_rlog_lib:import_batch(transaction, Transactions),
    {keep_state, D#d{next_batch_seqno = SeqNo + 1}, [{reply, ok}]};
handle_batch(St, {tlog_batch, {Agent, SeqNo, Transactions}},
             D = #d{ agent = Agent
                   , next_batch_seqno = SeqNo
                   }) when St =:= ?bootstrap orelse
                           St =:= ?local_replay ->
    %% Historical data is being replayed, realtime transactions should
    %% be buffered up for later consumption:
    ?tp(rlog_replica_store_batch,
        #{ agent => Agent
         , seqno => SeqNo
         , transactions => Transactions
         }),
    buffer_tlog_ops(Transactions, D),
    {keep_state, D#d{next_batch_seqno = SeqNo + 1}, [{reply, ok}]};
handle_batch(_State, {Agent, SeqNo, _},
             #d{ agent = Agent
               , next_batch_seqno = MySeqNo
               }) when SeqNo > MySeqNo ->
    %% Gap in the TLOG. Consuming it now will cause inconsistency, so we must restart.
    %% TODO: sometimes it should be possible to restart gracefully to
    %% salvage the bootstrapped data.
    error(gap_in_the_tlog);
handle_batch(State, {Agent, SeqNo, _Transactions}, Data) ->
    ?tp(warning, rlog_replica_unexpected_batch,
        #{ state => State
         , from => Agent
         , seqno => SeqNo
         }),
    keep_state_and_data.

-spec initiate_bootstrap(data()) -> fsm_result().
initiate_bootstrap(D = #d{shard = Shard, remote_core_node = Remote}) ->
    {ok, Pid} = ekka_rlog_bootstrapper:start_link_client(Shard, Remote, self()),
    {keep_state, D#d{tmp_worker = Pid}}.

-spec handle_bootstrap_complete(ekka_rlog_server:checkpoint(), data()) -> fsm_result().
handle_bootstrap_complete(Checkpoint, D) ->
    ?tp(notice, "Bootstrap of the shard is complete",
        #{ shard => D#d.shard
         , checkpoint => Checkpoint
         }),
    {next_state, ?local_replay, D#d{ tmp_worker = undefined
                                   , checkpoint = Checkpoint
                                   }}.

-spec handle_agent_down(state(), term(), data()) -> fsm_result().
handle_agent_down(State, Reason, D) ->
    ?tp(notice, "Remote RLOG agent died",
        #{ reason => Reason
         , repl_state => State
         , shard => D#d.shard
         }),
    case State of
        ?normal ->
            {next_state, ?disconnected, D#d{agent = undefined}};
        _ ->
            %% TODO: Sometimes it should be possible to handle it more gracefully
            exit(agent_died)
    end.

-spec init_local_replay(data()) -> fsm_result().
init_local_replay(D) ->
    %% TODO: Not implemented
    {keep_state, D}.

-spec complete_initialization(data()) -> fsm_result().
complete_initialization(D) ->
    ?tp(notice, "Shard replica is ready",
        #{ shard => D#d.shard
         }),
    {next_state, ?normal, D#d{tmp_worker = undefined}}.

%% @private Try connecting to a core node
-spec handle_reconnect(data()) -> fsm_result().
handle_reconnect(#d{shard = Shard, checkpoint = Checkpoint}) ->
    case try_connect(Shard, Checkpoint) of
        {ok, _BootstrapNeeded = true, Node, ConnPid} ->
            D = #d{ shard            = Shard
                  , agent            = ConnPid
                  , remote_core_node = Node
                  },
            {next_state, ?bootstrap, D};
        {ok, _BootstrapNeeded = false, Node, ConnPid} ->
            D = #d{ shard            = Shard
                  , agent            = ConnPid
                  , remote_core_node = Node
                  , checkpoint       = Checkpoint
                  },
            {next_state, ?normal, D};
        {error, Err} ->
            ReconnectTimeout = application:get_env(ekka, rlog_replica_reconnect_interval, 5000),
            {keep_state_and_data, [{timeout, ReconnectTimeout, reconnect}]}
    end.

-spec try_connect(ekka_rlog:shard(), ekka_rlog_server:checkpoint()) ->
                {ok, boolean(), node(), pid()}
              | {error, term()}.
try_connect(Shard, Checkpoint) ->
    try_connect(shuffle(ekka_rlog:core_nodes()), Shard, Checkpoint).

-spec try_connect([node()], ekka_rlog:shard(), ekka_rlog_server:checkpoint()) ->
                {ok, boolean(), node(), pid()}
              | {error, term()}.
try_connect([], _, _) ->
    {error, no_core_available};
try_connect([Node|Rest], Shard, Checkpoint) ->
    case ekka_rlog:subscribe_tlog(Shard, Node, self(), Checkpoint) of
        {Err, _} when Err =:= badrpc orelse Err =:= badtcp ->
            try_connect(Rest, Shard, Checkpoint);
        {NeedBootstrap, Agent} when is_pid(Agent) ->
            {ok, NeedBootstrap, Node, Agent}
    end.

-spec buffer_tlog_ops([ekka_rlog_lib:tx()], data()) -> ok.
buffer_tlog_ops(Batch, Data) ->
    ok. %% TODO

-spec handle_worker_down(state(), term(), data()) -> no_return().
handle_worker_down(State, Reason, D) ->
    ?tp(critical, "Failed to initialize replica",
        #{ state => State
         , reason => Reason
         , shard => D#d.shard
         , worker => D#d.tmp_worker
         }),
    exit(bootstrap_failed).

handle_unknown(EventType, Event, State, Data) ->
    ?tp(warning, "rlog agent received unknown event",
        #{ event_type => EventType
         , event => Event
         , state => State
         , data => Data
         }),
    keep_state_and_data.

handle_state_trans(OldState, State, _Data) ->
    ?tp(rlog_agent_state_change,
        #{ from => OldState
         , to => State
         }),
    keep_state_and_data.

-spec shuffle([A]) -> [A].
shuffle(A) ->
    A. %% TODO: implement me
