-module(ekka_isolated_heal).

-export([active/0, status/0, start/3, report/1, event/3, tick/2, result/3, decide/3, accept_handoff/1, authorize_manual/4, close_shadow/1, restore_shadow/0, shadowed_owner/1]).

-define(KEY, {?MODULE, round}).
-define(SHADOW, {?MODULE, shadow}).
-define(TIMEOUT, 1500).
-define(OP_TIMEOUT, 30000).

active() -> status() =/= undefined.

status() -> get(?KEY).

start(C, Round, B) ->
    save(#{round => Round, target => C, peer => B, stage => confirm_isolation,
           tried => false, reports_seen => false, retries => 3}).

report(Node) ->
    case get(?KEY) of
        #{stage := Stage} = State when Stage =/= releasing ->
            save(State#{reports_seen => true, last_reporter => Node});
        _ -> ok
    end.

event(Event, Peer, Reports) ->
    case get(?KEY) of
        #{stage := Stage, peer := Peer} = State
          when Stage =:= repaired; Stage =:= repairing; Stage =:= repair_unconfirmed;
               Stage =:= verifying ->
            observe_peer(Event, Reports, State);
        _ ->
            ok
    end.

observe_peer(nodedown, Reports, #{stage := Stage} = State) ->
    save(State#{peer_down => #{reports => Reports, verified_before_cut => Stage =:= repaired}});
observe_peer(mnesia_down, _Reports, State) ->
    save(State#{split_view => ekka_mnesia:cluster_view()});
observe_peer(inconsistent_database, _Reports,
             #{peer := Peer, peer_down := Prior, split_view := {Running, _}} = State) ->
    case lists:member(Peer, Running) of
        false -> save(State#{new_partition => Prior});
        true -> ok
    end;
observe_peer(_Event, _Reports, _State) ->
    ok.

tick(Reports, Delay) ->
    State = get(?KEY),
    case State of
        #{shadow := true, manual_coordinator := Coordinator} when Coordinator =:= node() ->
            case owner_is_current(State) of
                true -> plan(refresh_target(State), Reports, Delay);
                false -> fence_manual_attempt(State)
            end;
        #{shadow := true, round := Round} ->
            shadow_alarm(State),
            close_shadow(Round);
        #{handed_off := true} ->
            finish_if_closed(State);
        #{owner_fenced := true} ->
            freeze(State, Reports);
        #{manual_coordinator := Coordinator} when Coordinator =:= node() ->
            case owner_is_current(State) of
                true -> plan(refresh_target(State), Reports, Delay);
                false -> fence_manual_attempt(State)
            end;
        #{round := {Owner, _}} when Owner =:= node() ->
            case owner_is_current(State) of
                true -> plan_with_current_target(refresh_target(State), Reports, Delay);
                false -> freeze(State, Reports)
            end
    end.

fence_manual_attempt(#{stage := revoking} = State) ->
    case plan(State, #{}, 0) of
        keep -> finish_if_closed(State);
        Result -> Result
    end;
fence_manual_attempt(#{stage := Stage, permit := Permit, target := C,
                       round := Round} = State)
  when Stage =:= retrying; Stage =:= releasing ->
    case target_status(State) of
        #{phase := ready, permit := Permit} ->
            revoke(C, Round, Permit, State),
            keep;
        #{phase := isolated, permit := Permit} ->
            manual(State);
        _ ->
            finish_if_closed(State)
    end;
fence_manual_attempt(State) ->
    finish_if_closed(State).

plan_with_current_target(#{owner_fenced := true} = State, Reports, _Delay) ->
    freeze(State, Reports);
plan_with_current_target(State, Reports, Delay) ->
    plan(State, Reports, Delay).

refresh_target(#{stage := confirm_isolation} = State) ->
    State;
refresh_target(#{stage := Stage, isolated_instance := Instance} = State) ->
    case target_status(State) of
        #{owner_fenced := true} when not is_map_key(manual, State) ->
            Fenced = State#{owner_fenced => true},
            save(Fenced),
            Fenced;
        #{phase := boot_wait, instance := NewInstance, running := no, mnesia := undefined}
          when NewInstance =/= Instance ->
            NextStage = resumed_stage(Stage),
            Next = State#{stage => NextStage, isolated_instance => NewInstance, tried => true},
            save(Next),
            alarm(isolated_waiting_for_admin, Next),
            Next;
        #{phase := isolated, cancelled := true, running := no}
          when Stage =:= waiting; Stage =:= retrying, not is_map_key(manual, State) ->
            Next = State#{stage => manual, tried => true},
            save(Next),
            alarm(isolated_waiting_for_admin, Next),
            Next;
        _ ->
            State
    end.

resumed_stage(repairing) -> repairing;
resumed_stage(verifying) -> verifying;
resumed_stage(repair_unconfirmed) -> repair_unconfirmed;
resumed_stage(repaired) -> repaired;
resumed_stage(_) -> manual.

freeze(#{peer := B, target := C, round := Round} = State, Reports) ->
    alarm(isolation_owner_unknown, State),
    PeerLeader = rpc:call(B, ekka_membership, leader_excluding, [[C]], ?TIMEOUT),
    case PeerLeader =:= B andalso
         rpc:call(C, ekka_heal_recovery, fence_owner, [Round], ?TIMEOUT) =:= ok of
        true ->
            Fenced = State#{owner_fenced => true},
            save(Fenced),
            Request = {autoheal_recovery_handoff, Fenced, Reports},
            try gen_server:call({ekka_node_monitor, B}, Request, ?TIMEOUT) of
                ok ->
                    save(Fenced#{handed_off => true}),
                    alarm(clear, Fenced),
                    report_alarm(clear, Fenced);
                _ -> ok
            catch
                exit:_ -> ok
            end;
        false ->
            ok
    end,
    keep.

accept_handoff(#{round := Round, peer := Peer} = Incoming) when Peer =:= node() ->
    case get(?KEY) of
        undefined ->
            save_shadow(Incoming);
        #{round := Round, shadow := true, manual_coordinator := _} ->
            ok;
        #{round := Round, shadow := true} ->
            save_shadow(Incoming);
        _ ->
            {error, recovery_already_active}
    end.

save_shadow(State) ->
    Shadow = State#{shadow => true},
    persistent_term:put(?SHADOW, Shadow),
    save(Shadow),
    shadow_alarm(Shadow).

shadow_alarm(#{owner_reports := Reports} = State) ->
    alarm(isolation_owner_unknown, State),
    case map_size(Reports) > 0 of
        true -> report_alarm(report_repair_pending, State);
        false -> ok
    end.

restore_shadow() ->
    case persistent_term:get(?SHADOW, undefined) of
        undefined -> false;
        Shadow -> save(Shadow), true
    end.

shadowed_owner(Owner) ->
    case persistent_term:get(?SHADOW, undefined) of
        #{round := {Owner, _}} -> true;
        _ -> false
    end.

authorize_manual(C, Round, Instance, Reports) ->
    case get(?KEY) of
        #{target := C, round := Round, shadow := true} = State ->
            authorize_shadow_manual(State, Instance, Reports);
        #{target := C, round := Round, handed_off := true, peer := Peer} = State
          when map_size(Reports) =:= 0 ->
            start_shadow_manual(State, Instance, Peer);
        #{target := C, round := Round, peer := B, stage := Stage} = State
          when (Stage =:= repaired orelse Stage =:= manual), map_size(Reports) =:= 0 ->
            case {owner_is_current(State), target_status(State), maps:is_key(new_partition, State)} of
                {true, #{phase := Phase, instance := Instance}, false}
                  when Phase =:= isolated; Phase =:= boot_wait ->
                    start_manual_retry(State, Instance, B, C);
                _ ->
                    {error, manual_recovery_unconfirmed}
            end;
        _ ->
            {error, recovery_busy}
    end.

authorize_shadow_manual(#{round := {Original, _}, owner_reports := OwnerReports,
                          stage := Stage} = State, Instance, Reports)
  when map_size(OwnerReports) =:= 0, map_size(Reports) =:= 0 ->
    case Stage of
        manual -> start_shadow_manual(State, Instance, Original);
        releasing -> start_shadow_manual(State, Instance, Original);
        retrying when not is_map_key(manual_coordinator, State) ->
            start_shadow_manual(State, Instance, Original);
        repaired -> start_shadow_manual(State, Instance, Original);
        _ -> {error, recovery_busy}
    end;
authorize_shadow_manual(_State, _Instance, _Reports) ->
    {error, recovery_busy}.

start_shadow_manual(#{target := C} = State, Instance, Original) ->
    Next = State#{peer => Original, manual_coordinator => node()},
    case {owner_is_current(Next) andalso peer_manual_ready(Next), target_status(State)} of
        {true, #{instance := Instance, phase := Phase, owner_fenced := true}}
          when Phase =:= isolated; Phase =:= boot_wait ->
            start_manual_retry(Next, Instance, Original, C);
        _ ->
            {error, manual_recovery_unconfirmed}
    end.

start_manual_retry(#{round := Round} = State, Instance, B, C) ->
    Running = [node(), B],
    Member = case matches_views(Running, [C]) of
        true -> true;
        false ->
            case matches_views(Running, []) of
                true -> {join, node()};
                false -> unavailable
            end
    end,
    case Member of
        unavailable ->
            {error, cluster_view_unavailable};
        _ ->
            Manual = case State of
                #{manual_coordinator := Owner} -> {manual, Owner, make_ref()};
                _ -> {manual, make_ref()}
            end,
            Permit = {Round, Instance, Manual},
            OwnerState = maps:without([owner_fenced, handed_off, manual_unconfirmed], State),
            Next = OwnerState#{stage => retrying, permit => Permit, reports_seen => false,
                               isolated_instance => Instance, tried => true, manual => true},
            case maps:get(shadow, Next, false) of
                true -> save_shadow(Next);
                false -> ok
            end,
            operation(C, manual_retry, [Round, Permit, Member], Next)
    end.

close_shadow(Round) ->
    case get(?KEY) of
        #{round := Round, shadow := true} = State ->
            finish_if_closed(State);
        _ ->
            keep
    end.

finish_if_closed(State) ->
    case target_status(State) of
        #{phase := serving} ->
            finish(State);
        Status ->
            case retired_round(State, Status) of
                true -> finish(State);
                false when is_map_key(handed_off, State) -> keep;
                false -> alarm(isolation_owner_unknown, State), keep
            end
    end.

%% A subsequent tracked heal or a healthy restart without the isolation marker
%% proves that C retired this round.
retired_round(_State, {unconfirmed, #{round := _, phase := Phase}}) when Phase =/= corrupt ->
    true;
retired_round(#{isolated_instance := Instance},
              {unconfirmed, #{phase := normal, running := yes, mnesia := Mnesia,
                              instance := NewInstance}}) ->
    NewInstance =/= Instance andalso is_pid(Mnesia);
retired_round(_State, _Status) ->
    false.

plan(#{stage := confirm_isolation} = State, Reports, _Delay) ->
    case target_status(State) of
        #{phase := Phase, attempt := Attempt} = Status when Phase =:= isolated; Phase =:= boot_wait ->
            Stage = case {Phase, Attempt} of {isolated, 0} -> waiting; _ -> manual end,
            save(State#{stage => Stage, isolated_instance => maps:get(instance, Status)}),
            alarm(alarm_stage(Stage), State#{status => Status}),
            plan(State#{stage => Stage, isolated_instance => maps:get(instance, Status)},
                 Reports, 0);
        #{phase := serving} ->
            finish(State);
        Status ->
            alarm(isolation_unconfirmed, State#{status => Status}),
            keep
    end;
plan(#{stage := waiting, reports_seen := false, tried := false} = State, Reports, _Delay)
  when map_size(Reports) =:= 0 ->
    case isolated_view(State) of
        true ->
            check_decision(retry, State),
            keep;
        false ->
            alarm(isolation_unconfirmed, State),
            keep
    end;
plan(#{stage := retrying, reports_seen := true, permit := Permit, target := C,
       round := Round} = State, _Reports, _Delay) ->
    revoke(C, Round, Permit, State),
    keep;
plan(#{stage := retrying, permit := Permit, reports_seen := false} = State,
     _Reports, _Delay) ->
    case target_status(State) of
        #{phase := ready, permit := Permit, mnesia := Mnesia, mnesia_instance := Mnesia} ->
            case {peer_manual_ready(State), retry_view(State, Mnesia)} of
                {true, true} ->
                    check_decision(release, State),
                    keep;
                {false, _} ->
                    revoke(maps:get(target, State), maps:get(round, State), Permit, State),
                    keep;
                {true, false} ->
                    alarm(isolation_unconfirmed, State),
                    keep
            end;
        #{phase := isolated, permit := Permit} ->
            manual(State);
        #{phase := Phase, instance := Instance}
          when (Phase =:= isolated orelse Phase =:= boot_wait),
               map_get(isolated_instance, State) =:= Instance, is_map_key(manual_unconfirmed, State) ->
            %% A missing reply cannot retire a permit. Fence it on C before the next request.
            revoke(maps:get(target, State), maps:get(round, State), Permit, State),
            keep;
        _ ->
            keep
    end;
plan(#{stage := releasing, permit := Permit} = State, _Reports, _Delay) ->
    case target_status(State) of
        #{phase := serving, permit := Permit} -> finish(State);
        #{phase := isolated, permit := Permit} -> manual(State);
        Status ->
            case retired_round(State, Status) of
                true -> finish(State);
                false -> alarm(isolation_unconfirmed, State), keep
            end
    end;
plan(#{stage := revoking, permit := Permit} = State, _Reports, _Delay) ->
    case target_status(State) of
        #{phase := Phase, permit := Permit} when Phase =:= isolated; Phase =:= boot_wait ->
            manual(State);
        #{phase := serving, permit := Permit} ->
            finish(State);
        #{phase := Phase, instance := Instance}
          when (Phase =:= isolated orelse Phase =:= boot_wait orelse Phase =:= ready),
               map_get(isolated_instance, State) =:= Instance, is_map_key(revoke_unconfirmed, State) ->
            revoke(maps:get(target, State), maps:get(round, State), Permit, State),
            keep;
        _ -> alarm(isolation_unconfirmed, State), keep
    end;
plan(#{stage := waiting} = State, Reports, Delay) ->
    manual(State),
    plan(State#{stage => manual}, Reports, Delay);
plan(#{stage := manual, manual_coordinator := _} = State, Reports, _Delay) ->
    case map_size(Reports) > 0 of
        true -> report_alarm(report_repair_pending, State);
        false -> ok
    end,
    keep;
plan(#{stage := manual, peer := B, retries := Retries} = State, Reports, _Delay) ->
    case {maps:is_key(node(), Reports), maps:is_key(B, Reports), Retries > 0,
          repair_view(State)} of
        {true, true, true, true} ->
            Id = {node(), make_ref()},
            Next = maps:without([peer_down, split_view, new_partition], State),
            Repair = Next#{stage => repairing, repair => Id, snapshot => Reports,
                           retries => Retries - 1},
            save(Repair),
            report_alarm(report_repair_pending, Repair),
            operation(B, heal_peer, [Id, maps:get(round, Repair)], Repair),
            keep;
        _ when map_size(Reports) > 0 ->
            report_alarm(report_repair_pending, State),
            keep;
        _ ->
            keep
    end;
plan(#{stage := verifying, peer := B, repair := Id, receipt := #{mnesia := Instance},
       snapshot := Snapshot} = State, Reports, Delay) ->
    Receipt = rpc:call(B, ekka_heal_recovery, receipt, [Id], ?TIMEOUT),
    case Receipt of
        {ok, #{mnesia := Instance, source := Source}} when Source =:= node() ->
            case isolated_view(State) andalso owner_is_current(State) andalso
                 rpc:call(B, erlang, whereis, [mnesia_monitor], ?TIMEOUT) =:= Instance of
                true ->
                    save(State#{stage => repaired}),
                    report_alarm(clear, State),
                    {consume, Snapshot};
                false ->
                    plan_new_partition(State, Reports, Delay)
            end;
        _ ->
            report_alarm(report_repair_pending, State#{verification => Receipt}),
            keep
    end;
plan(#{stage := repair_unconfirmed, peer := Peer, repair := Id} = State, _Reports, _Delay) ->
    case rpc:call(Peer, ekka_heal_recovery, receipt, [Id], ?TIMEOUT) of
        {ok, Receipt} -> save(State#{stage => verifying, receipt => Receipt});
        _ -> ok
    end,
    keep;
plan(#{stage := repaired, new_partition := _} = State, Reports, Delay) ->
    plan_new_partition(State, Reports, Delay);
plan(#{stage := repaired} = State, Reports, _Delay) when map_size(Reports) > 0 ->
    report_alarm(partition_evidence_pending, State#{retained => Reports}),
    keep;
plan(_State, _Reports, _Delay) ->
    keep.

plan_new_partition(#{new_partition := #{reports := Prior} = Evidence, peer := Peer} = State,
                   Reports, Delay) ->
    case received_after(node(), Prior, Reports) andalso received_after(Peer, Prior, Reports) andalso
         verified_partition(Evidence, State) andalso repair_view(State) of
        true ->
            Next = State#{stage => manual, retries => 3},
            save(Next),
            plan(Next, Reports, Delay);
        false ->
            report_alarm(partition_evidence_pending, State),
            keep
    end;
plan_new_partition(State, _Reports, _Delay) ->
    report_alarm(report_repair_pending, State),
    keep.

verified_partition(#{verified_before_cut := true}, _State) ->
    true;
verified_partition(_, #{peer := Peer, repair := Id, receipt := #{mnesia := Instance}}) ->
    %% Before receipt verification, B must have observed the same link going down
    %% while serving the exact Mnesia instance named by the receipt.
    case rpc:call(Peer, ekka_heal_recovery, receipt, [Id], ?TIMEOUT) of
        {ok, #{mnesia := Instance, owner_disconnected := true}} -> true;
        _ -> false
    end.

check_decision(Operation, #{round := Round} = State) ->
    save(State#{stage => {checked, Operation}}),
    self() ! {autoheal, {recovery_decision, Operation, Round}},
    ok.

decide(Operation, Round, Reports) ->
    case get(?KEY) of
        #{round := Round, stage := {checked, Operation}} = State ->
            decide_checked(Operation, State, Reports);
        _ ->
            ok
    end.

decide_checked(retry, #{reports_seen := false, target := C, round := Round,
                        isolated_instance := Instance} = State, Reports)
  when map_size(Reports) =:= 0 ->
    Permit = {Round, Instance, 1},
    Next = State#{stage => retrying, permit => Permit, tried => true},
    operation(C, retry, [Round, Permit], Next);
decide_checked(retry, State, _Reports) ->
    manual(State),
    ok;
decide_checked(release, #{reports_seen := false, target := C, round := Round,
                          permit := Permit, manual := true} = State, _Reports) ->
    operation(C, release, [Round, Permit], State#{stage => releasing});
decide_checked(release, #{reports_seen := false, target := C, round := Round,
                          permit := Permit} = State, Reports)
  when map_size(Reports) =:= 0 ->
    operation(C, release, [Round, Permit], State#{stage => releasing});
decide_checked(release, #{target := C, round := Round, permit := Permit} = State, _Reports) ->
    revoke(C, Round, Permit, State).

revoke(C, Round, Permit, State) ->
    operation(C, revoke, [Round, Permit], maps:remove(revoke_unconfirmed, State#{stage => revoking})).

received_after(Node, Prior, Reports) ->
    maps:get(Node, Reports, -1) > maps:get(Node, Prior, -1).

result(Key, Operation, Result) ->
    case get(?KEY) of
        #{operation := Key} = State -> handle_result(Operation, Result, State);
        _ -> ok
    end.

handle_result(heal_peer, {ok, #{mnesia := _} = Receipt}, State) ->
    save(State#{stage => verifying, receipt => Receipt});
handle_result(heal_peer, {error, {guard_refused, Reason}}, State) ->
    case ekka_mnesia:transient_refusal(Reason) of
        true -> manual(State), ok;
        false -> failed(Reason, State)
    end;
handle_result(heal_peer, Error, State) ->
    failed(Error, State);
handle_result(manual_retry, {error, Reason}, #{stage := retrying} = State)
  when Reason =:= stale_permit; Reason =:= owner_changed ->
    _ = manual(State),
    ok;
handle_result(manual_retry, {badrpc, Reason}, #{stage := retrying} = State) ->
    save(State#{manual_unconfirmed => Reason});
handle_result(revoke, {badrpc, Reason}, #{stage := revoking} = State) ->
    save(State#{revoke_unconfirmed => Reason});
handle_result(release, {error, owner_changed}, #{stage := releasing, manual := true} = State) ->
    save(State#{stage => retrying});
handle_result(_Operation, _Result, _State) ->
    ok.

failed(Error, State) ->
    save(State#{stage => repair_unconfirmed, result => Error}),
    report_alarm(report_repair_pending, State#{result => Error}).

operation(Node, Operation, Args, State) ->
    Key = make_ref(),
    save(State#{operation => Key}),
    Monitor = self(),
    _ = spawn(fun() ->
        Result = case Operation =:= revoke orelse owner_is_current(State) of
            true -> run_operation(Node, Operation, Args);
            false -> {error, owner_changed}
        end,
        Monitor ! {autoheal, {recovery_result, Key, Operation, Result}}
    end),
    ok.

run_operation(Node, Operation, Args) ->
    case Operation of
        heal_peer ->
            [Id, Round] = Args,
            case ekka_autoheal:heal_recovery_peer(Node, Id, Round) of
                ok -> rpc:call(Node, ekka_heal_recovery, receipt, [Id], ?TIMEOUT);
                Error -> Error
            end;
        _ ->
            rpc:call(Node, ekka_heal_recovery, Operation, Args, ?OP_TIMEOUT)
    end.

manual(State) ->
    save(State#{stage => manual}),
    alarm(isolated_waiting_for_admin, State),
    keep.

finish(#{round := {Owner, _} = Round, peer := Peer} = State) ->
    _ = erase(?KEY),
    case maps:get(shadow, State, false) of
        true -> persistent_term:erase(?SHADOW);
        false -> ok
    end,
    alarm(clear, State),
    case Owner =:= node() andalso is_map_key(manual, State) of
        true -> {ekka_node_monitor, Peer} ! {autoheal, {close_recovery_shadow, Round}};
        false -> ok
    end,
    resume.

alarm_stage(waiting) -> isolated_waiting_for_retry;
alarm_stage(manual) -> isolated_waiting_for_admin.

owner_is_current(#{peer := B, target := C}) ->
    try
        ekka_membership:leader_excluding([C]) =:= node() andalso
            rpc:call(B, ekka_membership, leader_excluding, [[C]], ?TIMEOUT) =:= node()
    catch
        error:_ -> false
    end.

peer_manual_ready(#{manual_coordinator := _, peer := Peer, round := Round}) ->
    case rpc:call(Peer, ekka_autoheal, recovery_status, [], ?TIMEOUT) of
        #{recovery := #{round := Round, stage := Stage}, reports := Reports}
          when map_size(Reports) =:= 0 ->
            lists:member(Stage, [manual, repaired, retrying, releasing, revoking]);
        _ ->
            false
    end;
peer_manual_ready(_State) ->
    true.

target_status(#{target := C, round := Round}) ->
    case rpc:call(C, ekka_heal_recovery, status, [], ?TIMEOUT) of
        #{round := Round} = Status -> Status;
        Other -> {unconfirmed, Other}
    end.

isolated_view(#{target := C, peer := B} = State) ->
    isolated_target(State) andalso matches_views([node(), B], [C]).

repair_view(#{target := C, peer := B, new_partition := _} = State) ->
    A = node(),
    isolated_target(State) andalso
        case rpc:multicall([A, B], ekka_mnesia, cluster_view, [], ?TIMEOUT) of
            {[{[A], StoppedA}, {[B], StoppedB}], []} ->
                StoppedA =:= lists:sort([B, C]) andalso StoppedB =:= lists:sort([A, C]);
            _ ->
                matches_views([A, B], [C])
        end;
repair_view(State) ->
    isolated_view(State).

isolated_target(#{isolated_instance := Instance} = State) ->
    case target_status(State) of
        #{phase := Phase, instance := Instance, running := no, mnesia := undefined}
          when Phase =:= isolated; Phase =:= boot_wait ->
            true;
        _ ->
            false
    end.

retry_view(#{target := C, peer := B}, Mnesia) when is_pid(Mnesia) ->
    Nodes = [node(), B, C],
    matches_views(Nodes, []) andalso
        case rpc:call(C, mnesia, table_info, [emqx_route, load_node], ?TIMEOUT) of
            Source when Source =:= B; Source =:= node() -> true;
            _ -> false
        end;
retry_view(_, _) ->
    false.

matches_views(Running, Stopped) ->
    Nodes = lists:sort(Running),
    View = {Nodes, Stopped},
    case rpc:multicall(Nodes, ekka_mnesia, cluster_view, [], ?TIMEOUT) of
        {Views, []} -> Views =:= lists:duplicate(length(Nodes), View);
        _ -> false
    end.

save(State) ->
    Prev = get(?KEY),
    case Prev =:= State of
        true -> ok;
        false ->
            logger:notice("Autoheal coordinator state: ~p", [State]),
            put(?KEY, State),
            ok
    end.

alarm(Stage, State) -> update_alarm(isolation_alarm, Stage, State).
report_alarm(Stage, State) -> update_alarm(report_alarm, Stage, State).

update_alarm(Key, Stage, State) ->
    case {get({?MODULE, Key}), ekka:callback(heal_alarm)} of
        {Stage, _} ->
            ok;
        {Previous, {ok, Alarm}} ->
            _ = apply_alarm(Alarm, activate, Stage, State),
            _ = apply_alarm(Alarm, deactivate, Previous, State),
            put({?MODULE, Key}, Stage),
            ok;
        _ -> ok
    end.

apply_alarm(_Alarm, _Action, undefined, _State) -> ok;
apply_alarm(_Alarm, _Action, clear, _State) -> ok;
apply_alarm(Alarm, Action, Stage, #{target := C, round := Round} = State) ->
    Name = iolist_to_binary(io_lib:format("autoheal/~s/~p/~s", [C, Round, Stage])),
    try Alarm(Action, Name, State)
    catch
        Class:Reason ->
            logger:error("Could not update autoheal alarm ~s: ~p", [Name, {Class, Reason}])
    end.
