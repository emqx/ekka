%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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

-export([discover_and_join/1, unregister_node/0, aquire_lock/0, release_lock/0]).

-define(LOG(Level, Format, Args),
        lager:Level("Ekka(AutoCluster): " ++ Format, Args)).

-spec(discover_and_join(Fun :: fun() | mfa()) -> any()).
discover_and_join(Fun) ->
    with_strategy(
      fun(Mod, Options) ->
        case Mod:lock(Options) of
            ok -> 
                discover_and_join(Mod, Options),
                log_error("Unlock", Mod:unlock(Options));
            ignore ->
                timer:sleep(rand:uniform(3000)),
                discover_and_join(Mod, Options);
            {error, Reason} ->
                ?LOG(error, "AutoCluster stopped for lock error: ~p", [Reason])
        end
      end),
    run_callback(Fun).

-spec(unregister_node() -> ok).
unregister_node() ->
    with_strategy(
      fun(Mod, Options) ->
        log_error("Unregister", Mod:unregister(Options))
      end).

-spec(aquire_lock() -> ok | failed).
aquire_lock() ->
    case application:get_env(ekka, autocluster_lock) of
        undefined ->
            application:set_env(ekka, autocluster_lock, true);
        {ok, _} -> failed
    end.

-spec(release_lock() -> ok).
release_lock() ->
    application:unset_env(ekka, autocluster_lock).

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
    case Mod:discover(Options) of
        {ok, Nodes} ->
            maybe_join([N || N <- Nodes, ekka_node:is_aliving(N)]),
            log_error("Register", Mod:register(Options));
        {error, Reason} ->
            ?LOG(error, "Discovery error: ~p", [Reason])
    end.

maybe_join([]) ->
    ignore;

maybe_join(Nodes) ->
    case ekka_mnesia:is_node_in_cluster() of
        true  -> ignore;
        false -> join_with(find_oldest_node(Nodes))
    end.

join_with(false) ->
    ignore;
join_with(Node) ->
    ekka_cluster:join(Node).

find_oldest_node([Node]) ->
    Node;
find_oldest_node(Nodes) ->
   case rpc:multicall(Nodes, ekka_membership, local_member, []) of
       {Members, []} ->
           Member = ekka_membership:oldest(Members), Member#member.node;
       {_Views, BadNodes} ->
            ?LOG(error, "Bad nodes found: ~p", [BadNodes]), false
   end.

run_callback(Fun) when is_function(Fun) ->
    Fun();
run_callback({M, F, A}) ->
    erlang:apply(M, F, A).

log_error(Format, {error, Reason}) ->
    ?LOG(error, Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) ->
    ok.

