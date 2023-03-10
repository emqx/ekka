%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behavior(gen_server).

-include_lib("mria/include/mria.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([ enabled/0
        , run/1
        , unregister_node/0
        , core_node_discovery_callback/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        ]).

-define(SERVER, ?MODULE).

-define(LOG(Level, Format, Args),
        logger:Level("Ekka(AutoCluster): " ++ Format, Args)).

%% ms
-define(DISCOVER_AND_JOIN_RETRY_INTERVAL, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec enabled() -> boolean().
enabled() ->
    case ekka:env(cluster_discovery) of
        {ok, {manual, _}} -> false;
        {ok, _Strategy}   -> mria_config:role() =:= core;
        undefined         -> false
    end.

-spec run(atom()) -> ok | ignore.
run(App) ->
    ?tp(ekka_autocluster_run, #{app => App}),
    case enabled() andalso gen_server:start({local, ?SERVER}, ?MODULE, [App], []) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _}} ->
            ignore;
        false ->
            ignore
    end.

-spec unregister_node() -> ok | ignore.
unregister_node() ->
    with_strategy(
      fun(Mod, Options) ->
          log_error("Unregister", ekka_cluster_strategy:unregister(Mod, Options))
      end).

%% @doc Core node discovery used by mria by replicant nodes to find
%% the core nodes.
-spec core_node_discovery_callback() -> [node()].
core_node_discovery_callback() ->
    case ekka:env(cluster_discovery) of
        {ok, {manual, _}} ->
            [];
        {ok, {Strategy, Options}} ->
            Mod = strategy_module(Strategy),
            try ekka_cluster_strategy:discover(Mod, Options) of
                {ok, Nodes} ->
                    Nodes;
                {error, Reason} ->
                    ?LOG(error, "Core node discovery error: ~p", [Reason]),
                    []
            catch _:Err:Stack ->
                    ?LOG(error, "Core node discovery error ~p: ~p", [Err, Stack]),
                    []
            end;
        undefined ->
            []
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(s,
        { application :: atom()
        }).

init([App]) ->
    group_leader(whereis(init), self()),
    self() ! loop,
    {ok, #s{ application = App
           }}.

handle_info(loop, S = #s{application = App}) ->
    wait_application_ready(App, 10),
    JoinResult = discover_and_join(),
    case is_discovery_complete(JoinResult) of
        true ->
            ?tp(ekka_autocluster_complete, #{app => App}),
            {stop, normal, S};
        false ->
            timer:send_after(?DISCOVER_AND_JOIN_RETRY_INTERVAL, loop),
            {noreply, S}
    end;
handle_info(_, S) ->
    {noreply, S}.

is_discovery_complete(ignore) ->
    is_node_registered();
is_discovery_complete(JoinResult) ->
    %% Check if the node joined cluster?
    NodeInCluster = mria:cluster_nodes(all) =/= [node()],
    %% Possibly there are nodes outside the cluster; keep trying if
    %% so.
    NoNodesOutside = JoinResult =:= ok,
    Registered = is_node_registered(),
    ?tp(ekka_maybe_run_app_again,
        #{ node_in_cluster  => NodeInCluster
         , node_registered  => Registered
         , no_nodes_outside => NoNodesOutside
         }),
    Registered andalso NodeInCluster andalso NoNodesOutside.

handle_call(_Req, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Req, S) ->
    {noreply, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wait_application_ready(_App, 0) ->
    timeout;
wait_application_ready(App, Retries) ->
    case ekka_node:is_running(App) of
        true  -> ok;
        false -> timer:sleep(1000),
                 wait_application_ready(App, Retries - 1)
    end.

-spec discover_and_join() -> ok | ignore | error.
discover_and_join() ->
    with_strategy(
      fun(Mod, Options) ->
        try ekka_cluster_strategy:lock(Mod, Options) of
            ok ->
                discover_and_join(Mod, Options);
            ignore ->
                timer:sleep(rand:uniform(3000)),
                discover_and_join(Mod, Options);
            {error, Reason} ->
                ?LOG(error, "AutoCluster stopped for lock error: ~p", [Reason]),
                error
        after
            log_error("Unlock", ekka_cluster_strategy:unlock(Mod, Options))
        end
      end).

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

-spec discover_and_join(module(), ekka_cluster_strategy:options()) -> ok | ignore | error.
discover_and_join(Mod, Options) ->
    ?tp(ekka_autocluster_discover_and_join, #{mod => Mod}),
    try ekka_cluster_strategy:discover(Mod, Options) of
        {ok, Nodes} ->
            ?tp(ekka_autocluster_discover_and_join_ok, #{mod => Mod, nodes => Nodes}),
            {AliveNodes, DeadNodes} = lists:partition(
                                        fun ekka_node:is_aliving/1,
                                        Nodes),
            Res = maybe_join(AliveNodes),
            ?LOG(debug, "join result: ~p", [Res]),
            log_error("Register", ekka_cluster_strategy:register(Mod, Options)),
            case DeadNodes of
                [] ->
                    ?LOG(info, "no discovered nodes outside cluster", []),
                    case Res of
                        {error, _} -> error;
                        ok         -> ok;
                        ignore     -> ignore
                    end;
                [_ | _] ->
                    ?LOG(warning, "discovered nodes outside cluster: ~p", [DeadNodes]),
                    error
            end;
        {error, Reason} ->
            ?LOG(error, "Discovery error: ~p", [Reason]),
            error
    catch
        _:Error:Stacktrace ->
            ?LOG(error, "Discover error: ~p~n~p", [Error, Stacktrace]),
            error
    end.

-spec maybe_join([node()]) -> ignore | ok | {error, _}.
maybe_join([]) ->
    ignore;
maybe_join(Nodes0) ->
    Nodes = lists:usort(Nodes0),
    KnownNodes = lists:usort(mria:cluster_nodes(all)),
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

is_node_registered() ->
    Nodes = core_node_discovery_callback(),
    lists:member(node(), Nodes).

log_error(Format, {error, Reason}) ->
    ?LOG(error, Format ++ " error: ~p", [Reason]);
log_error(_Format, _Ok) -> ok.
