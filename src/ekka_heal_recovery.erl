-module(ekka_heal_recovery).

-export([ capability/0, status/0, locked/1, try_locked/1, begin_heal/1, complete/0, isolate/1
        , retry/2, revoke/2, release/2, operator_stop/0, boot_wait/0
        , startup_allowed/0, recovery_starting/0, application_starting/0, membership_allowed/0, session_attempt/0, session_allowed/1
        , receipt/1, observe_owner_disconnect/1, request_manual/0, manual_retry/3, fence_owner/1
        ]).

-define(KEY, {?MODULE, state}).
-define(MARKER, "autoheal-recovery").

capability() ->
    case ekka:callback(heal_isolation) of
        {ok, _} -> 1;
        undefined -> 0
    end.

locked(Action) ->
    global:trans({{?MODULE, transition}, self()}, Action, [node()]).

try_locked(Action) ->
    case global:trans({{?MODULE, transition}, self()}, Action, [node()], 0) of
        aborted -> {error, autoheal_busy};
        Result -> Result
    end.

status() ->
    State = state(),
    State#{mnesia => whereis(mnesia_monitor), running => mnesia:system_info(is_running)}.

begin_heal({Owner, _} = Id) ->
    Members = ekka_mnesia:cluster_nodes(all),
    Voters = case lists:member(Owner, Members) of
        true -> Members -- [node()];
        false -> []
    end,
    case save(#{phase => healing, round => Id, instance => instance(), attempt => 0, voters => Voters}) of
        ok -> watch_transition(Id, 0);
        Error -> Error
    end.

watch_transition(Round, Attempt) ->
    Worker = self(),
    _ = spawn(fun() ->
        Ref = monitor(process, Worker),
        receive
            {'DOWN', Ref, process, Worker, Reason} ->
                locked(fun() -> recover_interrupted_transition(Round, Attempt, Reason) end)
        end
    end),
    ok.

recover_interrupted_transition(Round, Attempt, Reason) ->
    case state() of
        #{round := Round, attempt := Attempt, phase := Phase}
          when Phase =:= healing; Phase =:= retrying; Phase =:= isolating ->
            isolate({interrupted_transition, Phase, Reason});
        _ ->
            ok
    end.

complete() ->
    State = state(),
    case file:delete(marker()) of
        ok ->
            put_state(State#{phase => serving, mnesia_instance => whereis(mnesia_monitor)}),
            ok;
        Error -> Error
    end.

isolate(Reason) ->
    State = state(),
    ok = save(State#{phase => isolating, failure => Reason}),
    StopApps = call(heal_isolation),
    StopMnesia = ekka_mnesia:isolate(),
    case {StopApps, StopMnesia, whereis(mnesia_monitor), mnesia:system_info(is_running)} of
        {ok, ok, undefined, no} ->
            ok = save((state())#{phase => isolated}),
            {ok, status()};
        Results ->
            logger:error("Autoheal isolation unconfirmed: ~p. Stop results: ~p", [State, Results]),
            {error, {isolation_unconfirmed, Results}}
    end.

retry(Round, Permit) ->
    locked(fun() ->
        case state() of
            #{phase := isolated, round := Round, instance := Instance, attempt := 0}
              when Permit =:= {Round, Instance, 1} ->
                State = (state())#{attempt => 1, permit => Permit},
                case owner_current(State, retrying) of
                    true ->
                        ok = save(State#{phase => retrying}),
                        ok = watch_transition(Round, 1),
                        run_retry(fun do_retry/0);
                    false ->
                        ok = save(State#{cancelled => true}),
                        {ok, status()}
                end;
            #{round := Round, permit := Permit} ->
                {ok, status()};
            _ ->
                {error, stale_permit}
        end
    end).

request_manual() ->
    case status() of
        #{phase := Phase, round := Round, instance := Instance} = State
          when Phase =:= isolated; Phase =:= boot_wait ->
            case manual_owner(State) of
                {ok, Owner} ->
                    rpc:call(Owner, ekka_autoheal, request_manual, [node(), Round, Instance], 10000);
                Error ->
                    Error
            end;
        State ->
            {error, {manual_recovery_unavailable, State}}
    end.

manual_owner(#{owner_fenced := true, voters := Voters}) ->
    case rpc:multicall(Voters, ekka_membership, leader_excluding, [[node()]], 1500) of
        {[Owner | _] = Leaders, []} ->
            case lists:member(Owner, Voters) andalso
                 Leaders =:= lists:duplicate(length(Voters), Owner) of
                true -> {ok, Owner};
                false -> {error, recovery_owner_unconfirmed}
            end;
        _ ->
            {error, recovery_owner_unconfirmed}
    end;
manual_owner(#{round := {Owner, _}}) ->
    {ok, Owner}.

manual_retry(Round, Permit, Member) ->
    locked(fun() ->
        case state() of
            #{round := Round, permit := Permit} ->
                {ok, status()};
            #{phase := Phase, round := Round, instance := Instance, attempt := Attempt} = State
              when Phase =:= isolated; Phase =:= boot_wait ->
                case Permit of
                    {Round, Instance, Manual} ->
                        case manual_permit_owner(Manual, State) of
                            {ok, Authorized} ->
                                start_manual_permit(Permit, Authorized, Phase, Attempt, Member);
                            Error ->
                                Error
                        end;
                    _ ->
                        {error, stale_permit}
                end;
            _ ->
                {error, stale_permit}
        end
    end).

manual_permit_owner({manual, _}, #{round := {Owner, _}} = State) ->
    case maps:get(coordinator, State, Owner) of
        Owner -> {ok, State};
        _ -> {error, stale_permit}
    end;
manual_permit_owner({manual, NewOwner, _}, #{owner_fenced := true, voters := Voters} = State) ->
    case lists:member(NewOwner, Voters) of
        true -> {ok, State#{coordinator => NewOwner}};
        false -> {error, stale_permit}
    end;
manual_permit_owner(_, _) ->
    {error, stale_permit}.

start_manual_permit(Permit, State, Phase, Attempt, Member) ->
    Next = State#{phase => retrying, attempt => Attempt + 1, permit => Permit},
    case not manual_permit_seen(Permit, State) andalso owner_current(Next, retrying) of
        true ->
            ok = save(remember_manual_permit(Permit, Next)),
            ok = watch_transition(maps:get(round, State), Attempt + 1),
            run_retry(fun() -> do_manual_retry(Phase, Member) end);
        false ->
            {error, stale_permit}
    end.

manual_permit_seen(Permit, State) ->
    maps:is_key(Permit, maps:get(manual_permits, State, #{})).

remember_manual_permit(Permit, State) ->
    Permits = maps:get(manual_permits, State, #{}),
    State#{manual_permits => Permits#{Permit => true}}.

do_manual_retry(Phase, Member) ->
    if Phase =:= boot_wait -> ok = call(heal_boot_prepare); true -> ok end,
    Result = case Member of
        true -> ekka_mnesia:start_for_recovery();
        {join, Node} -> ekka_mnesia:join_for_recovery(Node)
    end,
    StartApps = case Phase of
        boot_wait -> fun() -> call(heal_boot_start) end;
        _ -> fun ekka_cluster:reboot_for_recovery/0
    end,
    start_apps_after_mnesia(Result, StartApps).

do_retry() ->
    start_apps_after_mnesia(ekka_mnesia:start_for_recovery(), fun ekka_cluster:reboot_for_recovery/0).

start_apps_after_mnesia(ok, StartApps) ->
    ok = application_starting(),
    StartApps();
start_apps_after_mnesia(Error, _StartApps) ->
    Error.

run_retry(Start) ->
    Result = try Start() of
        Return -> Return
    catch
        Class:Reason:Stack -> {error, {Class, Reason, Stack}}
    end,
    finish_retry(Result).

finish_retry(ok) ->
    ok = save((state())#{phase => ready, mnesia_instance => whereis(mnesia_monitor)}),
    {ok, status()};
finish_retry(Error) ->
    isolate(Error).

revoke(Round, Permit) ->
    locked(fun() ->
        case state() of
            #{round := Round, permit := Permit, phase := Phase}
              when Phase =:= isolated; Phase =:= serving ->
                {ok, status()};
            #{round := Round, phase := Phase, instance := Instance} = State
              when Phase =:= isolated; Phase =:= boot_wait ->
                case Permit of
                    {Round, Instance, {manual, _}} ->
                        remember_revoked_permit(Permit, State);
                    {Round, Instance, {manual, _, _}} ->
                        remember_revoked_permit(Permit, State);
                    {Round, Instance, 1} ->
                        cancel_retry_before_start(State, Permit);
                    _ ->
                        {error, stale_permit}
                end;
            #{round := Round, permit := Permit, phase := ready} ->
                isolate({revoked, Permit});
            _ ->
                {error, stale_permit}
        end
    end).

remember_revoked_permit(Permit, State) ->
    case manual_permit_seen(Permit, State) of
        false ->
            Next = State#{permit => Permit, cancelled => true},
            ok = save(remember_manual_permit(Permit, Next));
        true ->
            ok
    end,
    {ok, status()}.

cancel_retry_before_start(#{phase := isolated, attempt := 0} = State, Permit) ->
    ok = save(State#{attempt => 1, permit => Permit, cancelled => true}),
    {ok, status()};
cancel_retry_before_start(_State, _Permit) ->
    {error, stale_permit}.

release(Round, Permit) ->
    locked(fun() ->
        case state() of
            #{round := Round, permit := Permit, phase := ready, mnesia_instance := Mnesia} ->
                case {owner_current(state(), releasing), runtime(Mnesia)} of
                    {true, {ok, _}} ->
                        ok = complete(),
                        {ok, status()};
                    {false, _} ->
                        isolate(owner_changed);
                    {true, Error} ->
                        Error
                end;
            #{round := Round, permit := Permit, phase := serving} ->
                {ok, status()};
            _ ->
                {error, stale_permit}
        end
    end).

fence_owner(Round) ->
    locked(fun() ->
        case state() of
            #{round := Round, phase := Phase} = State
              when Phase =/= serving, not is_map_key(coordinator, State) ->
                ok = save(State#{owner_fenced => true, cancelled => true}),
                case Phase of
                    ready ->
                        case isolate(owner_changed) of
                            {ok, _} -> ok;
                            Error -> Error
                        end;
                    _ ->
                        ok
                end;
            _ ->
                {error, stale_round}
        end
    end).

owner_current(#{owner_fenced := true, permit := {_, _, {manual, _}}} = State, Stage)
  when not is_map_key(coordinator, State) ->
    owner_current(maps:remove(owner_fenced, State), Stage);
owner_current(#{owner_fenced := true, coordinator := Owner,
                permit := {_, _, {manual, Owner, _}}} = State, Stage) ->
    owner_current(maps:remove(owner_fenced, State), Stage);
owner_current(#{owner_fenced := true}, _Stage) ->
    false;
owner_current(#{voters := []}, _Stage) ->
    true;
owner_current(#{round := {Original, _}, voters := Voters} = State, Stage) ->
    Owner = maps:get(coordinator, State, Original),
    case rpc:multicall(Voters, ekka_membership, leader_excluding, [[node()]], 1500) of
        {Leaders, []} ->
            Leaders =:= lists:duplicate(length(Voters), Owner) andalso
                owner_decision(Owner, State, Stage);
        _ ->
            false
    end.

owner_decision(Owner, #{round := Round, permit := Permit} = State, Stage) ->
    case rpc:call(Owner, ekka_autoheal, recovery_status, [], 1500) of
        #{recovery := #{round := Round, stage := Stage, permit := Permit, target := Target} = Decision}
          when Target =:= node(), not is_map_key(owner_fenced, Decision),
               not is_map_key(handed_off, Decision) ->
            case Permit of
                {_, _, {manual, Owner, _}} ->
                    maps:get(manual_coordinator, Decision, undefined) =:= Owner andalso
                        peer_has_no_reports(maps:get(voters, State), Owner, Round);
                _ ->
                    true
            end;
        _ ->
            false
    end.

peer_has_no_reports(Voters, Owner, Round) ->
    case Voters -- [Owner] of
        [Peer] ->
            case rpc:call(Peer, ekka_autoheal, recovery_status, [], 1500) of
                #{recovery := #{round := Round}, reports := Reports} -> map_size(Reports) =:= 0;
                _ -> false
            end;
        _ ->
            false
    end.

operator_stop() ->
    case state() of
        #{phase := Phase} when Phase =:= normal; Phase =:= serving ->
            ok;
        #{phase := corrupt} ->
            ok;
        #{attempt := Attempt} = State ->
            ok = save(State#{cancelled => true, attempt => max(1, Attempt)}),
            isolate(operator_stop)
    end.

boot_wait() ->
    %% A quarantine boot does not start Ekka or Mnesia. Queries work over distribution.
    _ = application:load(mnesia),
    put_state(read_marker()),
    try prepare_release_path()
    catch
        Class:Reason ->
            logger:error("Could not load the held release: ~p", [{Class, Reason}])
    end,
    logger:warning("EMQX autoheal startup is held: ~p", [status()]),
    ok.

prepare_release_path() ->
    case os:getenv("EMQX_AUTOHEAL_RELEASE_BOOT") of
        false -> ok;
        Boot ->
            {ok, Bin} = file:read_file(Boot ++ ".boot"),
            {script, _, Instructions} = binary_to_term(Bin),
            Prefix = filename:join([code:root_dir(), "lib"]),
            Paths = [expand_boot_path(P, Prefix) || {path, Ps} <- Instructions, P <- Ps],
            ok = code:add_pathsa(lists:usort(Paths)),
            _ = application:load(ekka),
            emqx_recovery:register_callbacks()
    end.

expand_boot_path("$ROOT/lib/" ++ Rest, Prefix) -> filename:join(Prefix, Rest);
expand_boot_path("$ERTS_LIB_DIR/" ++ Rest, Prefix) -> filename:join(Prefix, Rest);
expand_boot_path(Path, _) -> Path.

startup_allowed() ->
    case state() of
        #{phase := normal} -> true;
        #{phase := serving} -> true;
        #{phase := Phase, starting_mnesia := Mnesia} when Phase =:= healing; Phase =:= retrying ->
            is_pid(Mnesia) andalso whereis(mnesia_monitor) =:= Mnesia;
        _ -> false
    end.

recovery_starting() -> phase() =:= retrying.

application_starting() ->
    put_state((state())#{starting_mnesia => whereis(mnesia_monitor)}).

membership_allowed() ->
    case phase() of
        normal -> true;
        serving -> true;
        _ -> false
    end.

session_attempt() ->
    attempt(persistent_term:get(?KEY, undefined)).

attempt(#{round := Round, instance := Instance, attempt := Attempt} = State) ->
    {Round, Instance, maps:get(permit, State, Attempt)};
attempt(#{phase := corrupt}) -> blocked;
attempt(_) -> normal.

session_allowed(Attempt) ->
    State = persistent_term:get(?KEY, undefined),
    Attempt =:= attempt(State) andalso
        case State of
            undefined -> true;
            #{phase := normal} -> true;
            #{phase := serving} -> true;
            _ -> false
        end.

observe_owner_disconnect(Owner) ->
    case persistent_term:get(?KEY, undefined) of
        #{phase := serving, round := {Owner, _} = Id, mnesia_instance := Instance} ->
            persistent_term:put({?MODULE, owner_disconnected}, {Id, Instance});
        _ ->
            ok
    end.

receipt(Id) ->
    case state() of
        #{phase := serving, round := Id, mnesia_instance := Mnesia} ->
            case runtime(Mnesia) of
                {ok, Receipt} ->
                    Disconnected = persistent_term:get({?MODULE, owner_disconnected}, undefined),
                    {ok, Receipt#{owner_disconnected => Disconnected =:= {Id, Mnesia}}};
                Error ->
                    Error
            end;
        _ ->
            {error, stale_heal}
    end.

runtime(Mnesia) when is_pid(Mnesia) ->
    case {whereis(mnesia_monitor), call(heal_runtime)} of
        {Mnesia, {ok, Source}} ->
            case whereis(mnesia_monitor) of
                Mnesia -> {ok, #{mnesia => Mnesia, source => Source}};
                _ -> {error, replaced_mnesia}
            end;
        _ ->
            {error, service_unavailable}
    end;
runtime(_) ->
    {error, service_unavailable}.

call(Name) ->
    try
        {ok, Callback} = ekka:callback(Name),
        Callback()
    catch
        Class:Reason:Stack ->
            logger:error("Autoheal callback ~p failed: ~p", [Name, {Class, Reason, Stack}]),
            {error, {Class, Reason}}
    end.

phase() -> maps:get(phase, state()).

state() ->
    case persistent_term:get(?KEY, undefined) of
        undefined -> read_marker();
        State -> State
    end.

read_marker() ->
    case file:read_file(marker()) of
        {error, enoent} ->
            #{phase => normal, instance => instance()};
        {ok, Binary} ->
            try binary_to_term(Binary) of
                #{round := {Owner, Round}, instance := _, attempt := Attempt} = State
                  when is_atom(Owner), is_reference(Round), is_integer(Attempt) ->
                    maps:remove(starting_mnesia,
                                State#{phase => boot_wait, instance => instance(), cancelled => true});
                _ ->
                    corrupt_marker(invalid_contents)
            catch
                error:badarg -> corrupt_marker(invalid_contents)
            end;
        {error, Reason} ->
            corrupt_marker(Reason)
    end.

corrupt_marker(Reason) ->
    #{phase => corrupt, failure => Reason, instance => instance()}.

save(State) ->
    File = marker(),
    Tmp = File ++ ".tmp",
    case file:write_file(Tmp, term_to_binary(State), [sync]) of
        ok ->
            case file:rename(Tmp, File) of
                ok ->
                    put_state(State),
                    ok;
                Error -> Error
            end;
        Error -> Error
    end.

put_state(State) ->
    logger:notice("Autoheal recovery state: ~p", [State]),
    persistent_term:put(?KEY, State).

marker() -> filename:join(ekka_mnesia:data_dir(), ?MARKER).

instance() -> {node(), erlang:system_info(start_time), erlang:system_info(creation)}.
