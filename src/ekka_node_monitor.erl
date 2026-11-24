%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_node_monitor).

-behaviour(gen_server).

-include("ekka.hrl").

%% API
-export([start_link/0, stop/0]).

-export([partitions/0]).

%% Internal Exports
-export([cast/2, run_after/2]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          partitions :: list(node()),
          heartbeat  :: undefined | reference(),
          autoheal   :: ekka_autoheal:autoheal(),
          autoclean  :: ekka_autoclean:autoclean()
         }).

-define(SERVER, ?MODULE).
-define(CONFIRM_DELAY, 3000).
-define(CONFIRM_RETRY_KEY, {?MODULE, confirm_retry}).
-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Monitor): " ++ Format, Args)).

%% @doc Start the node monitor.
-spec(start_link() -> {ok, pid()} | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() -> gen_server:stop(?SERVER).

%% @doc Get partitions.
partitions() ->
    gen_server:call(?SERVER, partitions).

%% @private
cast(Node, Msg) ->
    gen_server:cast({?SERVER, Node}, Msg).

%% @private
run_after(Delay, Msg) ->
    erlang:send_after(Delay, ?SERVER, Msg).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    rand:seed(exsplus, erlang:timestamp()),
    net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    {ok, _} = mnesia:subscribe(system),
    lists:foreach(fun(N) -> self() ! {nodeup, N, []} end, nodes() -- [node()]),
    State = #state{partitions = [],
                   autoheal   = ekka_autoheal:init(),
                   autoclean  = ekka_autoclean:init()
                  },
    {ok, ensure_heartbeat(State)}.

handle_call(partitions, _From, State = #state{partitions = Partitions}) ->
    {reply, Partitions, State};

handle_call({autoheal_manual_recovery, Target, Round, Instance}, _From,
            State = #state{autoheal = Autoheal}) when Autoheal =/= undefined ->
    {reply, ekka_autoheal:authorize_manual(Target, Round, Instance), State};
handle_call(autoheal_recovery_status, _From, State) ->
    {reply, ekka_autoheal:local_recovery_status(), State};
handle_call({autoheal_recovery_handoff, Recovery, Reports}, _From,
            State = #state{autoheal = Autoheal}) when Autoheal =/= undefined ->
    case ekka_autoheal:proc(Autoheal) of
        undefined ->
            Accept = fun() -> ekka_autoheal:accept_recovery_handoff(Recovery, Reports) end,
            case ekka_heal_recovery:try_locked(Accept) of
                ok -> {reply, ok, autoheal_handle_msg({retained_reports, []}, State)};
                Error -> {reply, Error, State}
            end;
        _ ->
            {reply, {error, autoheal_busy}, State}
    end;
handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({heartbeat, _FromNode}, State) ->
    {noreply, State};

handle_cast({suspect, FromNode, TargetNode}, State) ->
    ?LOG(info, "Suspect from ~s: ~s~n", [FromNode, TargetNode]),
    spawn(fun() ->
            Status = case net_adm:ping(TargetNode) of
                         pong -> up;
                         pang -> down
                     end,
            cast(FromNode, {confirm, TargetNode, Status})
          end),
    {noreply, State};

handle_cast({confirm, TargetNode, Status}, State) ->
    ?LOG(info, "Confirm ~s ~s", [TargetNode, Status]),
    {noreply, State};

handle_cast(Msg = {report_partition, _Node}, State) ->
    {noreply, autoheal_handle_msg(Msg, State)};
handle_cast(Msg = {heal_partition, _SplitView}, State) ->
    {noreply, autoheal_handle_msg(Msg, State)};
handle_cast(Msg = {heal_cluster, _, _}, State) ->
    {noreply, autoheal_handle_msg(Msg, State)};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({nodeup, Node, _Info}, State) ->
    ekka_membership:node_up(Node),
    {noreply, State};

handle_info({nodedown, Node, _Info}, State) ->
    ekka_membership:node_down(Node),
    ok = ekka_heal_recovery:observe_owner_disconnect(Node),
    ok = ekka_autoheal:observe_partition(nodedown, Node),
    run_after(3000, {suspect, Node}),
    {noreply, State};

handle_info({suspect, Node}, State) ->
    case ekka_mnesia:running_nodes() -- [node(), Node] of
        [ProxyNode|_] ->
            cast(ProxyNode, {suspect, node(), Node});
        [] -> ignore
    end,
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_up, Node}},
            State = #state{partitions = Partitions}) ->
    ekka_membership:mnesia_up(Node),
    case lists:member(Node, Partitions) of
        false -> ok;
        true -> ekka_membership:partition_healed(Node)
    end,
    %% If there was an anymmetric cluster partition, we might need more
    %% autoheal iterations to completely bring the cluster back to normal.
    case ekka_autoheal:enabled() of
        {true, _} -> run_after(?CONFIRM_DELAY, confirm_partition);
        false -> ignore
    end,
    case lists:delete(Node, Partitions) of
        [] ->
            cancel_retry(),
            {noreply, State#state{partitions = []}};
        Remaining ->
            {noreply, State#state{partitions = Remaining}}
    end;

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    ekka_membership:mnesia_down(Node),
    ok = ekka_autoheal:observe_partition(mnesia_down, Node),
    {noreply, State};

handle_info({mnesia_system_event, {inconsistent_database, Context, Node}},
            State = #state{partitions = Partitions}) ->
    ?LOG(critical, "Network partition detected from node ~s: ~p", [Node, Context]),
    ekka_membership:partition_occurred(Node),
    ok = ekka_autoheal:observe_partition(inconsistent_database, Node),
    case ekka_autoheal:enabled() of
        {true, _} -> run_after(?CONFIRM_DELAY, confirm_partition);
        false -> ignore
    end,
    {noreply, State#state{partitions = lists:usort([Node | Partitions])}};

handle_info({mnesia_system_event, {mnesia_overload, Details}}, State) ->
    ?LOG(warning, "Mnesia overload: ~p", [Details]),
    {noreply, State};

handle_info({mnesia_system_event, Event}, State) ->
    ?LOG(error, "Mnesia system event: ~p", [Event]),
    {noreply, State};

%% A regular confirmation may have already reported while this timeout was queued.
handle_info({timeout, Ref, confirm_partition}, State) when is_reference(Ref) ->
    case get(?CONFIRM_RETRY_KEY) of
        Ref -> handle_info(confirm_partition, State);
        _ -> {noreply, State}
    end;

%% Confirm if we should report the partitions
handle_info(confirm_partition, State = #state{partitions = []}) ->
    cancel_retry(),
    {noreply, State};

handle_info(confirm_partition, State = #state{partitions = Partitions}) ->
    confirm_partition(Partitions),
    {noreply, State};

handle_info({autoheal, Msg}, State) ->
    {noreply, autoheal_handle_msg(Msg, State)};

handle_info(heartbeat, State) ->
    lists:foreach(fun(Node) ->
                      if Node =/= node() -> cast(Node, {heartbeat, node()});
                         true            -> ok
                      end
                  end, ekka_mnesia:cluster_nodes(all)),
    {noreply, ensure_heartbeat(State#state{heartbeat = undefined})};

handle_info(Msg = {'EXIT', Pid, _Reason}, State = #state{autoheal = Autoheal}) ->
    case ekka_autoheal:proc(Autoheal) of
        Pid -> {noreply, autoheal_handle_msg(Msg, State)};
        _   -> {noreply, State}
    end;

%% Autoclean Event.
handle_info(autoclean, State = #state{autoclean = AutoClean}) ->
    {noreply, State#state{autoclean = ekka_autoclean:check(AutoClean)}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% TODO: This function triggers a bug in dialyzer, where it forgets about some record fields.
-dialyzer({nowarn_function, [ensure_heartbeat/1]}).
ensure_heartbeat(State = #state{heartbeat = undefined}) ->
    Interval = rand:uniform(2000) + 2000,
    State#state{heartbeat = run_after(Interval, heartbeat)};

ensure_heartbeat(State) ->
    State.

autoheal_handle_msg(Msg, State = #state{autoheal = Autoheal}) ->
    State#state{autoheal = ekka_autoheal:handle_msg(Msg, Autoheal)}.

%% The leader may be temporarily unavailable when a partition is confirmed; keep
%% re-confirming until the partitions are gone or the report is delivered.
confirm_partition(Partitions) ->
    case ekka_autoheal:enabled() of
        false ->
            ?LOG(warning, "Autoheal is disabled, not reporting the partitions: ~p",
                 [Partitions]),
            cancel_retry();
        {true, _} ->
            Leader = reporting_leader(ekka_membership:leader()),
            case ekka_node:is_running(Leader, ekka) of
                true  -> send_report(Leader, Partitions);
                false -> schedule_retry(Partitions)
            end
    end.

reporting_leader(Leader) ->
    case ekka_node:is_running(Leader, ekka, 1000) of
        true -> Leader;
        false ->
            case rpc:call(Leader, ekka_heal_recovery, status, [], 1000) of
                #{phase := Phase} when Phase =:= isolated; Phase =:= boot_wait ->
                    ekka_membership:leader_excluding([Leader]);
                _ ->
                    Leader
            end
    end.

send_report(Leader, Partitions) ->
    case retry_pending() of
        true ->
            ?LOG(info, "Leader ~s is available again, reporting the partitions: ~p",
                 [Leader, Partitions]);
        false ->
            ok
    end,
    cancel_retry(),
    cast(Leader, {report_partition, node()}).

schedule_retry(Partitions) ->
    case retry_pending() of
        true ->
            ?LOG(debug, "Leader is still down, will retry to autoheal the partitions: ~p",
                 [Partitions]);
        false ->
            ?LOG(critical, "Leader is down, cannot autoheal the partitions: ~p", [Partitions])
    end,
    ensure_retry().

%% Retries are tracked in the process dictionary so that #state{} keeps its shape:
%% Ekka is hot-upgraded by load_module only, without code_change.
retry_pending() ->
    get(?CONFIRM_RETRY_KEY) =/= undefined.

ensure_retry() ->
    case get(?CONFIRM_RETRY_KEY) of
        undefined ->
            start_retry();
        Ref ->
            case erlang:read_timer(Ref) of
                false -> start_retry();
                _ -> ok
            end
    end,
    ok.

start_retry() ->
    Ref = erlang:start_timer(?CONFIRM_DELAY, self(), confirm_partition),
    put(?CONFIRM_RETRY_KEY, Ref).

cancel_retry() ->
    case get(?CONFIRM_RETRY_KEY) of
        undefined ->
            ok;
        Ref ->
            _ = erlang:cancel_timer(Ref),
            _ = erase(?CONFIRM_RETRY_KEY),
            ok
    end.
