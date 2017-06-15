%%%===================================================================
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(ekka_node_monitor).

-include("ekka.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, notify/1, subscribe/1, unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {guid, heartbeat_timer, subscribers = []}).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(subscribe(atom()) -> ok).
subscribe(What) ->
    gen_server:call(?SERVER, {subscribe, self(), What}).

-spec(unsubscribe(atom()) -> ok).
unsubscribe(What) ->
    gen_server:call(?SERVER, {unsubscribe, self(), What}).

%% Notify join or leave.
-spec(notify(join | leave) -> ok).
notify(Action) ->
    gen_server:call(?SERVER, {notify, Action}).

%% @private
cast(Node, Msg) ->
    gen_server:cast({?SERVER, Node}, Msg).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    rand:seed(exsplus),
    process_flag(trap_exit, true),
    ets:new(membership, [set, protected, named_table, {keypos, 2}]),
    net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    {ok, _} = mnesia:subscribe(system),
    lists:foreach(fun(N) -> self() ! {nodeup, N} end, nodes() -- [node()]),
    {ok, heartbeat(#state{guid = ekka_guid:gen()})}.

handle_call(local_member, _From, State = #state{guid = Guid}) ->
    {reply, #member{node = node(), guid = Guid}, State};

handle_call({subscribe, Pid, What}, _From, State = #state{subscribers = Subscribers}) ->
    case lists:keymember({Pid, What}, 1, Subscribers) of
        true  -> reply(ok, State);
        false -> MRef = erlang:monitor(process, Pid),
                 reply(ok, State#state{subscribers = [{{Pid, What}, MRef} | Subscribers]})
    end;

handle_call({unsubscribe, Pid, What}, _From, State = #state{subscribers = Subscribers}) ->
    case lists:keyfind({Pid, What}, 1, Subscribers) of
        {_, MRef} -> erlang:demonitor(MRef, [flush]),
                     Subscribers1 = lists:keydelete({Pid, What}, 1, Subscribers),
                     reply(ok, State#state{subscribers = Subscribers1});
        false     -> reply(ok, State)
    end;

handle_call({notify, Action}, _From, State = #state{guid = Guid}) ->
    lists:foreach(
      fun(Node) ->
        cast(Node, {notify, {node(), Guid}, Action})
      end, ekka_mnesia:cluster_nodes(running) -- [node()]),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({ping, {Node, Guid}}, State) ->
    %%TODO: Add or update a member?
    io:format("Ping from ~s:~p~n", [Node, Guid]),
    {noreply, State};

handle_cast({notify, {Node, Guid}, Action}, State) ->
    io:format("Notify from ~s:~p ~s~n", [Node, Guid, Action]),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, State) ->
    handle_info({nodeup, Node, []}, State);

handle_info({nodeup, Node, _Info}, State = #state{guid = Guid}) ->
    io:format("Nodeup ~s~n", [Node]),
    case ekka_mnesia:is_node_in_cluster(Node) of
        true  -> cast(Node, {ping, {node(), Guid}});
        false -> ok
    end,
    {noreply, State};

handle_info({nodedown, Node, Info}, State) ->
    io:format("Nodedown ~s: ~p~n", [Node, proplists:get_value(nodedown_reason, Info)]),
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_up, Node}}, State) ->
    io:format("Mnesia ~s up.~n", [Node]),
    case ets:lookup(membership, Node) of
        [Member] -> ets:insert(membership, Member#member{status = up});
        [] -> ets:insert(membership, #member{node = Node, status = up})
    end,
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    %%cast({suspect, Node}, 
    io:format("Mnesia ~s down.~n", [Node]),
    {noreply, State};

handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) ->
    io:format("Mnesia inconsistent_database event: ~p, ~p~n", [Context, Node]),
    %%TODO 1. Backup and restart
    %%TODO 2. Set master nodes?
    {noreply, State};

handle_info(heartbeat, State = #state{guid = Guid}) ->
    AliveNodes = [N || N <- ekka_mnesia:cluster_nodes(all),
                       lists:member(N, nodes())],
    lists:foreach(fun(Node) ->
                    cast(Node, {ping, {node(), Guid}})
                  end, AliveNodes),
    {noreply, heartbeat(State#state{heartbeat_timer = undefined})};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

heartbeat(State = #state{heartbeat_timer = undefined}) ->
    Interval = rand:uniform(2000) + 1000,
    Timer = erlang:send_after(Interval, self(), heartbeat),
    State#state{heartbeat_timer = Timer};

heartbeat(State) ->
    State.

reply(Reply, State) ->
    {reply, Reply, State}.

