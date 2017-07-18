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

-module(ekka_cluster_mcast).

-behaviour(gen_server).

-behaviour(ekka_cluster_strategy).

-import(proplists, [get_value/2, get_value/3]).

%% Cluster strategy Callbacks
-export([nodelist/1, register/1, unregister/1]).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(LOG(Level, Format, Args),
        lager:Level("Ekka(Mcast): " ++ Format, Args)).

-type(option() :: {addr, inet:ip_address()}
                | {ports, list(inet:port_number())}
                | {iface, inet:ip_address()}
                | {ttl, pos_integer()}
                | {loop, boolean()}
                | {senbuf, pos_integer()}
                | {recbuf, pos_integer()}
                | {buffer, pos_integer()}).

-record(state, {sock, addr, ports, seen}).

-spec(nodelist(list(option())) -> list(node())).
nodelist(Options) ->
    {ok, Pid} = ekka_cluster_sup:start_child(?MODULE, Options),
    gen_server:call(Pid, nodelist, 60000).

register(_Options) ->
    ignore.
    
unregister(_Options) ->
    ok.

start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

init(Options) ->
    Addr  = get_value(addr, Options),
    Ports = get_value(ports, Options),
    Loop  = get_value(loop, Options, true),
    TTL   = get_value(ttl, Options, 1),
    Iface = get_value(iface, Options, {0,0,0,0}),
    case udp_open(Ports, [{multicast_if, Iface},
                          {multicast_ttl, TTL},
                          {multicast_loop, Loop},
                          {add_membership, {Addr, Iface}}]) of
        {ok, Sock} ->
            {ok, #state{sock = Sock, addr = Addr, ports = Ports, seen = []}};
        {error, Error} ->
            {stop, Error}
    end.

handle_call(nodelist, From, State = #state{sock = Sock, addr = Addr, ports = Ports}) ->
    Ping = {ping, node(), cookie_hash()},
    lists:foreach(fun(Port) ->
                    udp_send(Sock, Addr, Port, Ping)
                  end, Ports),
    erlang:send_after(5000, self(), {reply, nodelist, From}),
    {noreply, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected reqeust: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({reply, nodelist, From}, State = #state{seen = Seen}) ->
    gen_server:reply(From, Seen),
    {noreply, State#state{seen = []}};

handle_info({udp, Sock, Ip, InPort, Data}, State = #state{sock = Sock, seen = Seen}) ->
    inet:setopts(Sock, [{active, 1}]),
    MyCookie = cookie_hash(),
    {noreply, try binary_to_term(Data) of
                   {_Tag, Node, _Cookie} when Node =:= node() ->
                       State;
                   {ping, Node, Cookie} when Cookie == MyCookie ->
                       udp_send(Sock, Ip, InPort, {pong, node(), MyCookie}),
                       State#state{seen = lists:usort([Node | Seen])};
                   {pong, Node, Cookie} when Cookie == MyCookie ->
                       State#state{seen = lists:usort([Node | Seen])};
                   {_Tag, Node, Cookie} ->
                       ?LOG(warning, "Node ~s found with different cookie: ~p", [Node, Cookie]),
                       State;
                   OtherData ->
                       ?LOG(error, "Unexpected data received: ~p", [OtherData]),
                       State
               catch
                   error:badarg ->
                       ?LOG(error, "Corrupt Data: ~p", [Data]),
                       State
               end};

handle_info({udp_closed, Sock}, State = #state{sock = Sock}) ->
    {stop, udp_closed, State};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{sock = Sock}) ->
    gen_udp:close(Sock).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

udp_open([], _Options) ->
    {error, eaddrinuse};
    
udp_open([Port|Ports], Options) ->
    case gen_udp:open(Port, [binary, {active, 10}, {reuseaddr, true} | Options]) of
        {ok, Sock} ->
            {ok, Sock};
        {error, eaddrinuse} ->
            ?LOG(warning, "Multicast Adddress in use: ~p", [Port]),
            udp_open(Ports, Options);
        {error, Reason} ->
            {error, Reason}
    end.

udp_send(Sock, Addr, Port, Term) ->
    gen_udp:send(Sock, Addr, Port, term_to_binary(Term)).

cookie_hash() ->
    erlang:phash2(erlang:get_cookie()).

