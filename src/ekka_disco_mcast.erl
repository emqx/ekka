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

-module(emqx_mcast_socket).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-export([start_link/2, members/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(ping, {cluster, node, cookie}).

-record(pong, {cluster, node, ping_node, cookie}).

-record(member, {addr, port, seqno = 0, last}).

-record(state, {cluster, socket, addr, port, iface, loop, ttl,
                period, timer, seqno = 0, cookie, members}).

-type(mcast_option() :: {period, pos_integer()}
                      | {addr, inet:ip_address()}
                      | {port, inet:port_number()}
                      | {iface, inet:ip_address()}
                      | {ttl, pos_integer()}
                      | {loop, boolean()}
                      | {senbuf, pos_integer()}
                      | {recbuf, pos_integer()}
                      | {buffer, pos_integer()}).

-spec(start_link(Cluster, Options) -> {ok, pid()} | ignore | {error, term()} when
    Cluster :: string(),
    Options :: list(mcast_option())).
start_link(Cluster, Options) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [Cluster, Options], []).

-spec(members() -> [{node(), {inet:ip_address(), inet:port_number()}, erlang:timestamp()}]).
members() -> gen_server:call(?MODULE, members).

init([Cluster, Options]) ->
    rand:seed(exsplus),
    Addr = opt(addr, Options),
    Port = opt(port, Options),
    Loop = opt(loop, Options, true),
    TTL = opt(ttl, Options, 1),
    Iface = opt(iface, Options, {0,0,0,0}),
    Period = opt(period, Options, 1000),
    Cookie = erlang:phash2(erlang:get_cookie()),
    State = #state{cluster = Cluster, addr = Addr, port = Port,
                   iface = Iface, loop = Loop, ttl = TTL,
                   period = Period, cookie = Cookie,
                   members = gb_trees:empty()},
    case gen_udp:open(Port, [inet, binary, {active, once},
                             {multicast_if, Iface},
                             {multicast_ttl, TTL},
                             {multicast_loop, Loop},
                             {add_membership, {Addr, Iface}}]) of
        {ok, Socket} ->
            {ok, ping(State#state{socket = Socket})};
        {error, Error} ->
            {stop, Error}
    end.

opt(K, Opts) ->
    proplists:get_value(K, Opts).
opt(K, Opts, V) ->
    proplists:get_value(K, Opts, V).

handle_call(members, _From, State = #state{members = Members}) ->
    {reply, [{Node, {Ip, Port}, Ts} ||
               {Node, #member{addr = Ip, port = Port, last = Ts}}
                 <- gb_trees:to_list(Members)], State};

handle_call(_Req, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info({udp, Socket, Ip, InPort, Data},
            State = #state{cluster = Cluster, socket = Socket, cookie = Cookie}) ->
    inet:setopts(Socket, [{active, once}]),
    case catch binary_to_term(Data) of
        #ping{cluster = Cluster, node = Node, cookie = Cookie} when Node == node() ->
            {noreply, State};
        #ping{cluster = Cluster, node = Node, cookie = Cookie} ->
            {noreply, handle_ping({Ip, InPort}, Node, State)};
        #pong{cluster = Cluster, node = Node, cookie = Cookie} when Node == node() ->
            {noreply, State};
        #pong{cluster = Cluster, node = Node, ping_node = _PingNode, cookie = Cookie} ->
            {noreply, handle_pong({Ip, InPort}, Node, State)};
        {'EXIT', Error} ->
            error_logger:error_msg("Bad Multicast Packet from (): ~p, error: ~p", [Data, Error]),
            {noreply, State};
        _Msg ->
            {noreply, State}
    end;

handle_info({timeout, Ref, ping}, State = #state{seqno = SeqNo, timer = Ref}) ->
    %%TODO: check if node is timeout?
	{noreply, ping(State#state{seqno = SeqNo + 1})};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

ping(State = #state{cluster = Cluster, socket = Socket, addr = Addr,
                    port = Port, period = Period, cookie = Cookie}) ->
    Ping = #ping{cluster = Cluster, node = node(), cookie = Cookie},
    gen_udp:send(Socket, Addr, Port, term_to_binary(Ping)),
    State#state{timer = ping_timer(Period)}.

ping_timer(Period) ->
    erlang:start_timer(Period + rand:uniform(Period), self(), ping).

handle_ping({Ip, Port}, Node, State) ->
    pong({Ip, Port}, Node, State),
    update_member(Node, {Ip, Port}, State).

handle_pong({Ip, Port}, Node, State) ->
    update_member(Node, {Ip, Port}, State).

pong({Ip, Port}, PingNode, #state{cluster = Cluster, socket = Socket, cookie = Cookie}) ->
    Pong = #pong{cluster = Cluster, node = node(), ping_node = PingNode, cookie = Cookie},
    gen_upd:send(Socket, Ip, Port, term_to_binary(Pong)).

update_member(Node, {Ip, Port}, State = #state{seqno = SeqNo, members = Members}) ->
    M = #member{addr = Ip, port = Port, seqno = SeqNo, last = erlang:timestamp()},
    case gb_trees:lookup(Node, Members) of
        {value, _M} ->
            State#state{members = gb_trees:update(Node, M, Members)};
        none ->
            State#state{members = gb_trees:insert(Node, M, Members)}
    end.

