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

-module(ekka_autocluster).

-include_lib("mria/include/mria.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([ enabled/0
        , run/1
        , unregister_node/0
        , core_node_discovery_callback/0
        ]).

-export([acquire_lock/1, release_lock/1]).

-define(LOG(Level, Format, Args),
        logger:Level("Ekka(AutoCluster): " ++ Format, Args)).

%% ms
-define(DISCOVER_AND_JOIN_RETRY_INTERVAL, 5000).

-spec(enabled() -> boolean()).
enabled() ->
    case ekka:env(cluster_discovery) of
        {ok, {manual, _}} -> false;
        {ok, _Strategy}   -> mria_config:role() =:= core;
        undefined         -> false
    end.

-spec(run(atom()) -> any()).
run(App) ->
    ?tp(ekka_autocluster_run, #{app => App}),
    case acquire_lock(App) of
        ok ->
            spawn(fun() ->
                      group_leader(whereis(init), self()),
                      wait_application_ready(App, 10),
                      JoinResult =
                        try
                            discover_and_join()
                        catch
                            _:Error:Stacktrace ->
                                ?LOG(error, "Discover error: ~p~n~p", [Error, Stacktrace]),
                                error
                        after
                            release_lock(App)
                        end,
                      maybe_run_again(App, JoinResult)
                  end);
        failed -> ignore
    end.

wait_application_ready(_App, 0) ->
    timeout;
wait_application_ready(App, Retries) ->
    case ekka_node:is_running(App) of
        true  -> ok;
        false -> timer:sleep(1000),
                 wait_application_ready(App, Retries - 1)
    end.

maybe_run_again(App, JoinResult) ->
    %% Check if the node joined cluster?
    NodeInCluster = mria_mnesia:is_node_in_cluster(),
    %% Possibly there are nodes outside the cluster; keep trying if
    %% so.
    NoNodesOutside = JoinResult =:= ok,
    Registered = is_node_registered(),
    ?tp(ekka_maybe_run_app_again,
        #{ app => App
         , node_in_cluster  => NodeInCluster
         , node_registered  => Registered
         , no_nodes_outside => NoNodesOutside
         }),
    case NodeInCluster andalso Registered andalso NoNodesOutside of
        true -> 
            ?LOG(notice, "discovery and join successfully", []),
            ok;
        false ->
            ?LOG(warning, "discovery did not succeed; retrying in ~p ms",
                 [?DISCOVER_AND_JOIN_RETRY_INTERVAL]),
            timer:sleep(?DISCOVER_AND_JOIN_RETRY_INTERVAL),
            run(App)
    end.

-spec(discover_and_join() -> any()).
discover_and_join() ->
    with_strategy(
      fun(Mod, Options) ->
        try Mod:lock(Options) of
            ok ->
                Res = discover_and_join(Mod, Options),
                log_error("Unlock", Mod:unlock(Options)),
                Res;
            ignore ->
                timer:sleep(rand:uniform(3000)),
                discover_and_join(Mod, Options);
            {error, Reason} ->
                ?LOG(error, "AutoCluster stopped for lock error: ~p", [Reason]),
                error
        after
            log_error("Unlock", Mod:unlock(Options))
        end
      end).

-spec(unregister_node() -> ok).
unregister_node() ->
    with_strategy(
      fun(Mod, Options) ->
          log_error("Unregister", Mod:unregister(Options))
      end).

-spec(acquire_lock(atom()) -> ok | failed).
acquire_lock(App) ->
    case application:get_env(App, autocluster_lock) of
        undefined ->
            application:set_env(App, autocluster_lock, true);
        {ok, _} -> failed
    end.

-spec(release_lock(atom()) -> ok).
release_lock(App) ->
    application:unset_env(App, autocluster_lock).

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
    ?tp(ekka_autocluster_discover_and_join, #{mod => Mod}),
    case Mod:discover(Options) of
        {ok, [Node]} when Node =:= node() -> ok;
        {ok, Nodes} ->
            ?tp(ekka_autocluster_discover_and_join_ok, #{mod => Mod, nodes => Nodes}),
            {AliveNodes, DeadNodes} = lists:partition(
                                        fun ekka_node:is_aliving/1,
                                        Nodes),
            Res = maybe_join(AliveNodes),
            ?LOG(debug, "join result: ~p", [Res]),
            log_error("Register", Mod:register(Options)),
            case DeadNodes of
                [] ->
                    ?LOG(info, "no discovered nodes outside cluster", []),
                    ok;
                [_ | _] ->
                    ?LOG(warning, "discovered nodes outside cluster: ~p", [DeadNodes]),
                    error
            end;
        {error, Reason} ->
            ?LOG(error, "Discovery error: ~p", [Reason]),
            error
    end.

maybe_join([]) ->
    ignore;
maybe_join(Nodes0) ->
    Nodes = lists:usort(Nodes0),
    KnownNodes = lists:usort(mria_mnesia:cluster_nodes(all)),
    case Nodes =:= KnownNodes of
        true  ->
            ?LOG(info, "all discovered nodes already in cluster; ignoring", []),
            ignore;
        false ->
            OldestNode = find_oldest_node(Nodes),
            ?LOG(info, "joining with ~p", [OldestNode]),
            join_with(OldestNode)
    end.

join_with(false) ->
    ignore;
join_with(Node) when Node =:= node() ->
    ignore;
join_with(Node) ->
    ekka_cluster:join(Node).

find_oldest_node([Node]) ->
    Node;
find_oldest_node(Nodes) ->
    case rpc:multicall(Nodes, mria_membership, local_member, [], 30000) of
        {ResL, []} ->
            case [M || M <- ResL, is_record(M, member)] of
                [] -> ?LOG(error, "Bad members found on nodes ~p: ~p", [Nodes, ResL]),
                      false;
                Members ->
                    (mria_membership:oldest(Members))#member.node
            end;
        {ResL, BadNodes} ->
            ?LOG(error, "Bad nodes found: ~p, ResL: ", [BadNodes, ResL]), false
   end.

%% @doc Core node discovery used by mria by replicant nodes to find
%% the core ones.
-spec(core_node_discovery_callback() -> [node()]).
core_node_discovery_callback() ->
    with_strategy(
      fun(Mod, Opts) ->
              case Mod:discover(Opts) of
                  {ok, Nodes} ->
                      Nodes;
                  {error, Reason} ->
                      ?LOG(error, "Core node discovery error: ~p", [Reason]),
                      []
              end
      end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_node_registered() ->
    Nodes = core_node_discovery_callback(),
    lists:member(node(), Nodes).

log_error(Format, {error, Reason}) ->
    ?LOG(error, Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) -> ok.
