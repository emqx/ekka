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

-module(ekka_node).

-behaviour(gen_server).

-import(lists, [concat/1]).

%% Node API
-export([is_aliving/1, is_running/2, parse_name/1]).

%% Delegate API
-export([delegate/2, forward/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {name, status, options}).

%%%===================================================================
%%% Node API
%%%===================================================================

%% @doc Is node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) ->
    lists:member(Node, nodes()) orelse net_adm:ping(Node) =:= pong.

%% @doc Is the application running?
-spec(is_running(node(), atom()) -> boolean()).
is_running(Node, App) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc, _}  -> false;
        Applications -> lists:keymember(App, 1, Applications)
    end.

%% @doc Parse node name
-spec(parse_name(string()) -> atom()).
parse_name(Name) when is_list(Name) ->
    case string:tokens(Name, "@") of
        [_Node, _Host] -> list_to_atom(Name);
        _              -> with_host(Name)
    end.

with_host(Name) ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    list_to_atom(concat([Name, "@", Host])).

%%%===================================================================
%%% Delegate API
%%%===================================================================

%% @doc Start the delegate
-spec(delegate(node(), list(ekka:delegate_option())) ->
      {ok, pid()} | ignore | {error, any()}).
delegate(Node, Options) ->
    gen_server:start_link(?MODULE, [Node, Options], []).

%% @doc Forward message to a node
-spec(forward(node(), any()) -> ok | {error, any()}).
forward(Node, Msg) ->
    case ekka_node_sup:delegate(Node) of %% TODO: performance issue?
        {ok, Pid} -> gen_server:cast(Pid, {forward, Node, Msg});
        false     -> {error, no_delegate}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Node, Options]) ->
    erlang:monitor_node(Node, true),
    Status = case lists:member(Node, nodes()) of
                 true  -> up;
                 false -> down
             end,
    {ok, #state{name = Node, status = Status, options = Options}}.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({forward, Node, Msg}, State) ->
    io:format("Forward to ~s: ~p~n", [Node, Msg]),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, State = #state{name = Node}) ->
    {noreply, State#state{status = up}};

handle_info({nodedown, Node}, State = #state{name = Node}) ->
    {noreply, State#state{status = down}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{name = Node}) ->
    erlang:monitor_node(Node, false), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

