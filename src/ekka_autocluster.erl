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

-export([start/1]).

-define(LOG(Level, Format, Args),
        lager:Level("Ekka(AutoCluster): " ++ Format, Args)).

-spec(start(Callback :: fun() | mfa()) -> any()).
start(Callback) ->
    with_strategy(
      fun(Mod, Options) ->
        case Mod:lock(Options) of
            ok -> 
                discover_and_join(Mod, Options);
            ignore ->
                timer:sleep(rand:uniform(5000)),
                discover_and_join(Mod, Options);
            {error, Reason} ->
                ?LOG(error, "AutoCluster stopped for lock error: ~p", [Reason])
        end,
        Mod:unlock(Options)
      end),
    run_callback(Callback).

with_strategy(Fun) ->
    case ekka:env(cluster_discovery) of
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

discover_and_join(Mod, Options) ->
    Nodes = Mod:discover(Options),
    maybe_join([N || N <- Nodes, ekka_node:is_aliving(N)]),
    Mod:register(Options).

maybe_join([]) ->
    ignore;

maybe_join(Nodes) ->
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
            ?LOG(error, "Bad Nodes found when autocluster: ~p", [BadNodes]),
            false
   end.

run_callback(Fun) when is_function(Fun) ->
    Fun();
run_callback({M, F, A}) ->
    erlang:apply(M, F, A).

