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

-module(ekka_autocluster).

-include("ekka.hrl").

-export([bootstrap/0, maybe_join/1, maybe_register/0, maybe_unregister/0]).

-define(LOG(Level, Format, Args), lager:Level("Ekka(AutoCluster): " ++ Format, Args)).

-spec(bootstrap() -> ok | ignore).
bootstrap() ->
    with_strategy(
      fun(Mod, Options) ->
          maybe_join([N || N <- Mod:nodelist(Options), N =/= node()])
      end).

maybe_join([]) ->
    ignore;

maybe_join(Nodes) ->
    ?LOG(info, "Join ~p", [Nodes]),
    case ekka_mnesia:is_node_in_cluster() of
        true  -> ignore;
        false -> case find_oldest_node(Nodes) of
                     false -> ignore;
                     Node  -> ekka_cluster:join(Node)
                 end
    end.

find_oldest_node([Node]) ->
    Node;
find_oldest_node(Nodes) ->
   case rpc:multicall(Nodes, ekka_membership, local_member, []) of
       {Members, []} ->
           Member = ekka_membership:oldest(Members), Member#member.node;
       {_Views, BadNodes} ->
            ?LOG(critical, "Bad Nodes found when autocluster: ~p", [BadNodes]),
            false
   end.

maybe_register() ->
    with_strategy(fun(Mod, Options) -> Mod:register(Options) end).

maybe_unregister() ->
    with_strategy(fun(Mod, Options) -> Mod:unregister(Options) end).

with_strategy(Fun) ->
    case application:get_env(ekka, cluster_discovery) of
        {ok, {manual, _}} ->
            ignore;
        {ok, {Strategy, Options}} ->
            Fun(strategy_module(Strategy), Options);
        undefined ->
            ignore
    end.

strategy_module(Strategy) ->
    case code:is_loaded(Strategy) of
        {file, _} -> Strategy; %% Provider?
        false     -> list_to_atom("ekka_cluster_" ++  atom_to_list(Strategy))
    end.

