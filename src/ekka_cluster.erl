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

%% @doc Cluster via Mnesia database.
-module(ekka_cluster).

-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Cluster): " ++ Format, Args)).

%% The target may go down between the entry check and the guarded join.
-define(JOIN_TARGET_TIMEOUT, 5000).

%% Applications are restarted after a membership change has committed. The application
%% start reserves the same membership lock again, so a transient rejection must be retried
%% instead of leaving Mnesia started and the applications stopped.
-define(RECOVERY_RETRY_DELAYS, [1000, 2000, 4000]).

-export([info/0, info/1]).

%% Cluster API
-export([ join/1
        , leave/0
        , force_leave/2
        , force_leave/1
        , status/1
        ]).

%% RPC call for Cluster Management
-export([ prepare/1
        , heal/1
        , reboot/0
        ]).

-type(info_key() :: running_nodes | stopped_nodes).

-type(infos() :: #{running_nodes := list(node()),
                   stopped_nodes := list(node())
                  }).

-export_type([info_key/0, infos/0]).

-spec(info(atom()) -> list(node())).
info(Key) -> maps:get(Key, info()).

-spec(info() -> infos()).
info() -> ekka_mnesia:cluster_info().

%% @doc Cluster status of the node.
status(Node) -> ekka_mnesia:cluster_status(Node).

%% @doc Join the cluster
-spec(join(node()) -> ok | ignore | {error, term()}).
join(Node) when Node =:= node() ->
    ignore;
join(Node) when is_atom(Node) ->
    with_join_target(Node, fun() ->
        ekka_mnesia:join_and_reboot(Node, fun() -> prepare_join(Node) end,
            fun() -> reboot(fun ekka:start_for_application/0) end)
    end).

%% Runs inside the membership guard, before prepare/1 stops the local application: the
%% target can go down while the join waits for a concurrent membership change.
prepare_join(Node) ->
    with_join_target(Node, fun() -> prepare(join) end).

with_join_target(Node, Action) ->
    case ekka_mnesia:is_node_in_cluster(Node) of
        true ->
            {error, {already_in_cluster, Node}};
        false ->
            case ekka_node:is_running(Node, ekka, ?JOIN_TARGET_TIMEOUT) of
                true -> Action();
                false -> {error, {node_down, Node}}
            end
    end.

%% @doc Leave from the cluster.
-spec(leave() -> ok | {error, any()}).
leave() ->
    Result = ekka_mnesia:guarded(node(), [], fun() ->
        case ekka_mnesia:running_nodes() -- [node()] of
            [] -> {error, node_not_in_cluster};
            [_ | _] ->
                case prepare(leave) of
                    ok -> ekka_mnesia:leave_cluster();
                    Error -> Error
                end
        end
    end),
    maybe_reboot(Result).

%% @doc Force a node leave from cluster.
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
force_leave(Node) ->
    force_leave(Node, fun() -> true end).

force_leave(Node, _Check) when Node =:= node() ->
    ignore;
force_leave(Node, Check) ->
    Result = ekka_mnesia:guarded(Node, [], fun() ->
        case Check() of
            true -> do_force_leave(Node);
            false -> ignore
        end
    end),
    case Result of
        reboot -> rpc:call(Node, ?MODULE, reboot, []);
        _ -> Result
    end.

do_force_leave(Node) ->
    case ekka_mnesia:is_node_in_cluster(Node)
         andalso rpc:call(Node, ?MODULE, prepare, [leave]) of
        ok ->
            case ekka_mnesia:remove_from_cluster(Node) of
                ok    -> reboot;
                Error -> Error
            end;
        false ->
            ekka_membership:announce({force_leave, Node}),
            {error, node_not_in_cluster};
        {badrpc, nodedown} ->
            case ekka_mnesia:remove_from_cluster(Node) of
                ok -> ekka_membership:announce({force_leave, Node});
                Error -> Error
            end;
        {badrpc, Reason} ->
            {error, Reason};
        {error, _} = Error -> Error
    end.

%% @doc Heal partitions
-spec(heal(shutdown_and_reboot) -> ok | {error, term()}).
heal(shutdown_and_reboot) ->
    maybe_reboot(ekka_mnesia:restart_membership(fun() -> prepare(heal) end)).

%% The Mnesia stop/start (or the membership change) is committed at this point; only the
%% application start can be retried.
maybe_reboot(ok) -> reboot();
maybe_reboot(Error) -> Error.

%% @doc Prepare to join or leave the cluster.
-spec(prepare(join | leave | heal) -> ok | {error, term()}).
prepare(Action) ->
    ekka_membership:announce(Action),
    case ekka:callback(prepare) of
        {ok, Prepare} -> Prepare(Action);
        undefined     -> application:stop(ekka)
    end.

%% @doc Reboot after join or leave cluster.
%% Retry a transient refusal of application startup without repeating the membership action.
-spec(reboot() -> ok | {error, term()}).
reboot() ->
    reboot(fun ekka:start/0).

reboot(Default) ->
    try retry_guard_refusal(fun() -> do_reboot(Default) end, application_start, ?RECOVERY_RETRY_DELAYS) of
        ok ->
            ok;
        {error, Reason} ->
            restart_failed(Reason);
        Other ->
            restart_failed({bad_return, Other})
    catch
        throw:{error, {guard_refused, _} = Reason} ->
            restart_failed(Reason)
    end.

retry_guard_refusal(Action, Stage, Delays) ->
    case {Action(), Delays} of
        {{error, {guard_refused, Reason}} = Error, [Delay | Rest]} ->
            case ekka_mnesia:transient_refusal(Reason) of
                true ->
                    ?LOG(warning, "Recovery stage ~p was refused: ~p; retrying in ~p ms.",
                         [Stage, Reason, Delay]),
                    timer:sleep(Delay),
                    retry_guard_refusal(Action, Stage, Rest);
                false ->
                    Error
            end;
        {Result, _} ->
            Result
    end.

do_reboot(Default) ->
    case ekka:callback(reboot) of
        {ok, Reboot} -> Reboot();
        undefined    -> Default()
    end.

restart_failed(Reason) ->
    ?LOG(error, "Applications were not restarted after the membership change: ~p. The node "
                "does not serve until they are restarted; a refusal log on the affected "
                "node names the lock holder if the membership guard refused the start.",
         [Reason]),
    {error, {application_restart_failed, Reason}}.
