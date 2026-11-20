%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_mnesia).

-include("ekka.hrl").

-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Mnesia): " ++ Format, Args)).

%% Start and stop mnesia
-export([ start/0
        , start_for_application/0
        , restart/1
        , restart_membership/1
        , guarded/3
        , guarded_refusal/3
        , transient_refusal/1
        , join_cluster/2
        , join_and_reboot/3
        , ensure_started/0
        , ensure_stopped/0
        , connect/1
        ]).

%% Mnesia Cluster API
-export([ join_cluster/1
        , leave_cluster/0
        , remove_from_cluster/1
        , cluster_info/0
        , cluster_status/1
        , cluster_view/0
        , cluster_nodes/1
        , running_nodes/0
        ]).

-export([ is_node_in_cluster/0
        , is_node_in_cluster/1
        ]).

%% Dir, schema and tables
-export([ data_dir/0
        , copy_schema/1
        , delete_schema/0
        , del_schema_copy/1
        , create_table/2
        , copy_table/1
        , copy_table/2
        ]).

-export([ diagnosis/1
        , diagnosis_tab/1
        ]).

-deprecated({copy_table, 1, next_major_release}).

%%--------------------------------------------------------------------
%% Start and init mnesia
%%--------------------------------------------------------------------

%% @doc Start mnesia database
%% A membership guard refusal is reported as {error, {guard_refused, Reason}} so the
%% application start can be retried.
-spec(start() -> ok | {error, term()}).
start() ->
    guarded_refusal(node(), [], fun start_mnesia/0).

-spec(start_for_application() -> ok | {error, term()}).
start_for_application() ->
    case joined_mnesia_is_running() of
        true -> ok;
        false -> start()
    end.

joined_mnesia_is_running() ->
    case application:get_env(ekka, joined_mnesia) of
        {ok, {Joiner, Monitor}} when is_pid(Joiner), is_pid(Monitor) ->
            whereis(mnesia_monitor) =:= Monitor andalso is_process_alive(Joiner) andalso
                mnesia:system_info(is_running) =:= yes;
        _ -> false
    end.

start_mnesia() ->
    ensure_ok(ensure_data_dir()),
    ensure_ok(init_schema()),
    ok = mnesia:start(),
    init_tables(),
    wait_for(tables).

%% @private
ensure_data_dir() ->
    case filelib:ensure_dir(data_dir()) of
        ok              -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Data dir
-spec(data_dir() -> string()).
data_dir() -> mnesia:system_info(directory).

%% @doc Ensure mnesia started
-spec(ensure_started() -> ok | {error, any()}).
ensure_started() ->
    guarded(node(), [], fun start_and_wait/0).

restart(Prepare) ->
    guarded(node(), [], fun() ->
        ensure_ok(Prepare()),
        ensure_ok(ensure_stopped()),
        start_and_wait()
    end).

%% Restart mnesia for a membership change. A guard refusal happened before the local
%% application was stopped, so it is reported as {error, {guard_refused, Reason}} and can
%% be retried; an error after the stop/start started keeps its own shape.
restart_membership(Prepare) ->
    guarded_refusal(node(), [], fun() ->
        ensure_ok(Prepare()),
        ensure_ok(ensure_stopped()),
        start_and_wait()
    end).

start_and_wait() ->
    ok = mnesia:start(), wait_for(start).

%% @doc Ensure mnesia stopped
-spec(ensure_stopped() -> ok | {error, any()}).
ensure_stopped() ->
    stopped = mnesia:stop(), wait_for(stop).

%% @private
%% @doc Init mnesia schema or tables.
init_schema() ->
    case mnesia:system_info(extra_db_nodes) of
        []    -> mnesia:create_schema([node()]);
        [_|_] -> ok
    end.

%% @private
%% @doc Init mnesia tables.
init_tables() ->
    case mnesia:system_info(extra_db_nodes) of
        []    -> create_tables();
        [_|_] -> copy_tables()
    end.

%% @doc Create mnesia tables.
create_tables() ->
    ekka_boot:apply_module_attributes(boot_mnesia).

%% @doc Copy mnesia tables.
copy_tables() ->
    ekka_boot:apply_module_attributes(copy_mnesia).

%% @doc Create mnesia table.
-spec(create_table(Name:: atom(), TabDef :: list()) -> ok | {error, any()}).
create_table(Name, TabDef) ->
    ensure_tab(mnesia:create_table(Name, TabDef)).

%% @doc Copy mnesia table.
-spec(copy_table(Name :: atom()) -> ok).
copy_table(Name) ->
    copy_table(Name, ram_copies).

-spec(copy_table(Name:: atom(), ram_copies | disc_copies) -> ok).
copy_table(Name, RamOrDisc) ->
    ensure_tab(mnesia:add_table_copy(Name, node(), RamOrDisc)).

%% @doc Copy schema.
copy_schema(Node) ->
    case mnesia:change_table_copy_type(schema, Node, disc_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, schema, Node, disc_copies}} ->
            ok;
        {aborted, Error} ->
            {error, Error}
    end.

%% @doc Force to delete schema.
delete_schema() ->
    mnesia:delete_schema([node()]).

%% @doc Delete schema copy
del_schema_copy(Node) ->
    case mnesia:del_table_copy(schema, Node) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Cluster mnesia
%%--------------------------------------------------------------------

%% @doc Join the mnesia cluster
-spec(join_cluster(node()) -> ok | {error, term()}).
join_cluster(Node) when Node =/= node() ->
    join_cluster(Node, fun() -> ok end).

join_cluster(Node, Prepare) when Node =/= node() ->
    guarded_join(Node, Prepare, fun() -> ok end).

join_and_reboot(Node, Prepare, Reboot) when Node =/= node() ->
    case guarded_join(Node, Prepare, fun() -> {joined, whereis(mnesia_monitor)} end) of
        {joined, Monitor} ->
            %% The application starts in a different process. Reuse the admitted Mnesia
            %% instance for this reboot while it still owns the replicated tables.
            Marker = {self(), Monitor},
            ok = application:set_env(ekka, joined_mnesia, Marker),
            try Reboot()
            after
                case application:get_env(ekka, joined_mnesia) of
                    {ok, Marker} -> application:unset_env(ekka, joined_mnesia);
                    _ -> ok
                end
            end;
        Error -> Error
    end.

guarded_join(Node, Prepare, Joined) ->
    guarded(node(), [Node], fun() ->
        case Prepare() of
            ok -> ok = do_join(Node), Joined();
            Error -> Error
        end
    end).

%% The guard either runs the action and returns its result, or refuses it with
%% {error, Reason} without changing anything.
-spec guarded(node(), [node()], fun(() -> T)) -> T | {error, term()}.
guarded(Node, Targets, Action) ->
    guarded(Node, Targets, Action, fun(Reason) -> {error, Reason} end).

%% Same as guarded/3, but a refusal is reported as {error, {guard_refused, Reason}} so the
%% caller can tell it apart from an error returned by the action.
-spec guarded_refusal(node(), [node()], fun(() -> T)) -> T | {error, term()}.
guarded_refusal(Node, Targets, Action) ->
    Refusal = make_ref(),
    try guarded(Node, Targets, Action, fun(Reason) -> {Refusal, Reason} end) of
        {Refusal, Reason} -> {error, {guard_refused, Reason}};
        {error, {guard_refused, _} = Reason} -> {error, {membership_action_failed, Reason}};
        Result -> Result
    catch
        %% An ordinary throw is returned as a value by rpc:call.
        throw:{error, {guard_refused, _} = Reason} -> {error, {membership_action_failed, Reason}}
    end.

guarded(Node, Targets, Action, OnRefusal) ->
    case ekka:callback(membership_guard) of
        {ok, Guard} ->
            %% A synchronous guard may execute the action in another local worker.
            Marker = ets:new(membership_action, [public]),
            try
                Result = Guard(Node, Targets, fun() -> ets:insert(Marker, {ran}), Action() end),
                case {Result, ets:member(Marker, ran)} of
                    {{error, Reason}, false} ->
                        report_rejection(Node, Reason),
                        OnRefusal(Reason);
                    _ -> Result
                end
            after
                ets:delete(Marker)
            end;
        undefined ->
            Action()
    end.

%% A refusal is transient when the membership change is currently blocked and the same
%% operation can be attempted again. Any other refusal is reported to the caller without
%% retrying.
-spec transient_refusal(term()) -> boolean().
transient_refusal(busy) -> true;
transient_refusal({route_cleanup_busy, _Node}) -> true;
transient_refusal(cluster_view_changed) -> true;
transient_refusal(cluster_view_unavailable) -> true;
transient_refusal(cluster_lock_unavailable) -> true;
transient_refusal(_) -> false.

%% A refusal means the membership change did not even start. It is expected while another
%% membership change or a cleanup holds the cleanup lock for the node, or while the cluster
%% membership cannot be confirmed.
report_rejection(Node, Reason) ->
    ?LOG(warning, "Membership change for ~p was refused by the membership guard: ~p. "
                  "Nothing was changed. This is expected while another membership change or "
                  "a cleanup holds the cleanup lock for that node, or while the cluster "
                  "membership cannot be confirmed; retry once that operation completes. "
                  "Cleanup lock holders on ~p: ~p (pid and node).",
         [Node, Reason, node(), lock_holders(Node)]).

%% Diagnostic only: holders of the cleanup lock for Node registered on this node. The lock
%% is shared with the upper-layer route cleanup and is not necessarily held here.
lock_holders(Node) ->
    try
        case ets:lookup(global_locks, {route_cleanup, Node}) of
            [{_Resource, _Requester, Holders}] -> [{Pid, node(Pid)} || {Pid, _Ref} <- Holders];
            [] -> []
        end
    catch _:_ -> []
    end.

do_join(Node) ->
    %% Stop mnesia and delete schema first
    ensure_ok(ensure_stopped()),
    ensure_ok(delete_schema()),
    %% Start mnesia and cluster to node
    ensure_ok(start_and_wait()),
    ensure_ok(connect(Node)),
    ensure_ok(copy_schema(node())),
    %% Copy tables
    copy_tables(),
    ensure_ok(wait_for(tables)).

%% @doc Cluster Info
-spec(cluster_info() -> map()).
cluster_info() ->
    Running = mnesia:system_info(running_db_nodes),
    Stopped = mnesia:system_info(db_nodes) -- Running,
    #{running_nodes => lists:sort(Running),
      stopped_nodes => lists:sort(Stopped)
     }.

%% @doc Cluster status of the node
-spec(cluster_status(node()) -> running | stopped | false).
cluster_status(Node) ->
    case is_node_in_cluster(Node) of
        true ->
            case lists:member(Node, running_nodes()) of
                true  -> running;
                false -> stopped
            end;
        false -> false
    end.

-spec(cluster_view() -> {[node()], [node()]}).
cluster_view() ->
    Running = lists:sort(running_nodes()),
    {Running, lists:sort(cluster_nodes(all) -- Running)}.

%% @doc This node try leave the cluster
-spec(leave_cluster() -> ok | {error, any()}).
leave_cluster() ->
    case running_nodes() -- [node()] of
        [] ->
            {error, node_not_in_cluster};
        Nodes ->
            case lists:any(fun(Node) ->
                            case leave_cluster(Node) of
                                ok               -> true;
                                {error, _Reason} -> false
                            end
                          end, Nodes) of
                true  -> ok;
                false -> {error, {failed_to_leave, Nodes}}
            end
    end.

-spec(leave_cluster(node()) -> ok | {error, any()}).
leave_cluster(Node) when Node =/= node() ->
    case is_running_db_node(Node) of
        true ->
            ensure_ok(ensure_stopped()),
            ensure_ok(rpc:call(Node, ?MODULE, del_schema_copy, [node()])),
            ensure_ok(delete_schema());
            %%ensure_ok(start()); %% restart?
        false ->
            {error, {node_not_running, Node}}
    end.

%% @doc Remove node from mnesia cluster.
-spec(remove_from_cluster(node()) -> ok | {error, any()}).
remove_from_cluster(Node) when Node =/= node() ->
    case {is_node_in_cluster(Node), is_running_db_node(Node)} of
        {true, true} ->
            ensure_ok(rpc:call(Node, ?MODULE, ensure_stopped, [])),
            mnesia_lib:del(extra_db_nodes, Node),
            ensure_ok(del_schema_copy(Node)),
            ensure_ok(rpc:call(Node, ?MODULE, delete_schema, []));
        {true, false} ->
            case del_schema_copy(Node) of
                ok ->
                    mnesia_lib:del(extra_db_nodes, Node),
                    ok;
                Error -> Error
            end;
        {false, _} ->
            {error, node_not_in_cluster}
    end.

%% @doc Is this node in mnesia cluster?
is_node_in_cluster() ->
    ekka_mnesia:cluster_nodes(all) =/= [node()].

%% @doc Is the node in mnesia cluster?
-spec(is_node_in_cluster(node()) -> boolean()).
is_node_in_cluster(Node) when Node =:= node() ->
    is_node_in_cluster();
is_node_in_cluster(Node) ->
    lists:member(Node, cluster_nodes(all)).

%% @private
%% @doc Is running db node.
is_running_db_node(Node) ->
    lists:member(Node, running_nodes()).

%% @doc Cluster with node.
-spec(connect(node()) -> ok | {error, any()}).
connect(Node) ->
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, [Node]} -> ok;
        {ok, []}     -> {error, {failed_to_connect_node, Node}};
        Error        -> Error
    end.

%% @doc Running nodes.
-spec(running_nodes() -> list(node())).
running_nodes() -> cluster_nodes(running).

%% @doc Cluster nodes.
-spec(cluster_nodes(all | running | stopped) -> [node()]).
cluster_nodes(all) ->
    mnesia:system_info(db_nodes);
cluster_nodes(running) ->
    mnesia:system_info(running_db_nodes);
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running).

%% @private
ensure_ok(ok) -> ok;
ensure_ok({error, {Node, {already_exists, Node}}}) -> ok;
ensure_ok({badrpc, Reason}) -> throw({error, {badrpc, Reason}});
ensure_ok({error, Reason}) -> throw({error, Reason}).

%% @private
ensure_tab({atomic, ok})                             -> ok;
ensure_tab({aborted, {already_exists, _Name}})       -> ok;
ensure_tab({aborted, {already_exists, _Name, _Node}})-> ok;
ensure_tab({aborted, Error})                         -> Error.

%% @doc Wait for mnesia to start, stop or tables ready.
-spec(wait_for(start | stop | tables) -> ok | {error, Reason :: term()}).
wait_for(start) ->
    case mnesia:system_info(is_running) of
        yes      -> ok;
        no       -> {error, mnesia_unexpectedly_stopped};
        stopping -> {error, mnesia_unexpectedly_stopping};
        starting -> timer:sleep(1000), wait_for(start)
    end;

wait_for(stop) ->
    case mnesia:system_info(is_running) of
        no       -> ok;
        yes      -> {error, mnesia_unexpectedly_running};
        starting -> {error, mnesia_unexpectedly_starting};
        stopping -> timer:sleep(1000), wait_for(stop)
    end;

wait_for(tables) ->
    Tables = mnesia:system_info(local_tables),
    do_wait_for_tables(Tables).

do_wait_for_tables(Tables) ->
    case mnesia:wait_for_tables(Tables, 30000) of
        ok                   -> ok;
        {error, Reason}      -> {error, Reason};
        {timeout, BadTables} ->
            logger:warning("~p: still waiting for table(s): ~p", [?MODULE, BadTables]),
            catch diagnosis(BadTables),
            %% lets try to force reconnect all the db_nodes to get schema merged,
            %% mnesia_controller is smart enough to not force reconnect the node that is already connected.
            mnesia_controller:connect_nodes(mnesia:system_info(db_nodes)),
            do_wait_for_tables(BadTables)
    end.

-spec diagnosis([atom()]) -> ok.
diagnosis(BadTables) ->
    RunningNodes = mnesia:system_info(running_db_nodes),
    DBNodes = mnesia:system_info(db_nodes),
    Checks = [ %% Check Mnesia start stage
               { is_running, yes, fun mnesia_lib:is_running/0 }
               %% Check Mnesia schema merge with remote nodes
             , { is_schema_merged, true, fun() ->
                                                 case mnesia_controller:get_info(_Timeout = 5000) of
                                                     {info, State} ->
                                                         %% the state record is very stable since 2009
                                                         element(3, State);
                                                     {timeout, _} ->
                                                         timeout
                                                 end
                                         end}
               %% Check known down nodes. They where down already before this node get down(they are still down).
             , { known_down_nodes, [], fun mnesia_recover:get_mnesia_downs/0 }
               %% Nodes that suppose to be UP.
             , { down_nodes, [], fun() -> DBNodes -- RunningNodes end }
             ],

    GeneralInfo = lists:filtermap(fun({Item, Expected, Fun}) ->
                                        Res = Fun(),
                                        case  Res =:= Expected of
                                            true ->
                                                false;
                                            false ->
                                                {true, io_lib:format("Check ~p should get ~p but got ~p~n ",
                                                                     [Item, Expected, Res])}
                                        end
                                  end, Checks),
    PerTabInfo = lists:map(fun diagnosis_tab/1, BadTables),
    logger:warning(GeneralInfo ++ PerTabInfo),
    ok.

-spec diagnosis_tab(atom()) -> iolist().
diagnosis_tab(Tab) ->
    try
        Props = mnesia:table_info(Tab, all),
        TabNodes = proplists:get_value(all_nodes, Props),
        KnownDown = mnesia_recover:get_mnesia_downs(),
        LocalNode = node(),
        case proplists:get_value(load_node, Props) of
            unknown ->
                io_lib:format("Table ~p is waiting for one of the nodes: ~p ~n",
                              [Tab, (TabNodes--KnownDown)--[LocalNode]]);
            LocalNode ->
                io_lib:format("Table ~p is loading from local disc copy ~n", [Tab]);
            Node ->
                io_lib:format("Table ~p is loading from remote node ~p ~n", [Tab, Node])
        end
    catch _:_ ->
            %% Most likely schema is not merged with remote.
            io_lib:format("Not able to read table info for ~p ~n", [Tab])
    end.
