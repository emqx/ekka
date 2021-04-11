%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_rlog_props).

-export([ replicant_bootstrap_stages/2
        , all_batches_received/1
        , counter_import_check/3

        , check_sequence/1
        ]).

-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("eunit/include/eunit.hrl").

replicant_bootstrap_stages(Node, Trace0) ->
    Trace = ?of_domain([ekka, rlog, replica|_], Trace0),
    ?causality( #{?snk_kind := state_change, to := disconnected, ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := bootstrap,    ?snk_meta := #{pid := _Pid}}
              , Trace
              ),
    ?causality(# {?snk_kind := state_change, to := bootstrap,    ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := local_replay, ?snk_meta := #{pid := _Pid}}
              , Trace
              ),
    ?causality(# {?snk_kind := state_change, to := local_replay, ?snk_meta := #{pid := _Pid}}
              , #{?snk_kind := state_change, to := normal,       ?snk_meta := #{pid := _Pid}}
              , Trace
              ).

all_batches_received(Trace0) ->
    Trace = ?of_domain([ekka, rlog|_], Trace0),
    ?strict_causality(
        #{?snk_kind := rlog_realtime_op, agent := _A, seqno := _S}
      , #{?snk_kind := K, agent := _A, seqno := _S} when K =:= rlog_replica_import_trans;
                                                         K =:= rlog_replica_store_trans
      , Trace).

counter_import_check(CounterKey, Node, Trace) ->
    Writes = [element(3, Rec) || #{ ?snk_kind := rlog_import_trans
                                  , ?snk_meta := #{node := N}
                                  , ops := [{{test_tab, K}, Rec, write}]
                                  } <- Trace, K =:= CounterKey, N =:= Node],
    ?assert(length(Writes) > 0),
    ?assert(check_sequence(Writes)).

%% Check sequence of numbers. It should be increasing by no more than
%% 1, with possible restarts from an earler point. If restart
%% happened, then the last element of the sequence must be greater
%% than any other element seen before.
check_sequence([]) ->
    true;
check_sequence([First|Rest]) ->
    check_sequence(First, First, Rest).

check_sequence(Max, LastElem, []) ->
    LastElem >= Max orelse
        ?panic("invalid sequence restart",
               #{ maximum   => Max
                , last_elem => LastElem
                }),
    true;
check_sequence(Max, Prev, [Next|Rest]) when Next =:= Prev + 1 ->
    check_sequence(max(Max, Prev), Next, Rest);
check_sequence(Max, Prev, [Next|Rest]) when Next =< Prev ->
    check_sequence(max(Max, Prev), Next, Rest);
check_sequence(Max, Prev, [Next|_]) ->
    ?panic("gap in the sequence",
           #{ maximum => Max
            , elem => Prev
            , next_elem => Next
            }).

%%================================================================================
%% Unit tests
%%================================================================================

check_sequence_test() ->
    ?assert(check_sequence([])),
    ?assert(check_sequence([1, 2])),
    ?assert(check_sequence([2, 3, 4])),
    %% Gap:
    ?assertError(_, check_sequence([0, 1, 3])),
    ?assertError(_, check_sequence([0, 1, 13, 14])),
    %% Replays:
    ?assert(check_sequence([1, 1, 2, 3, 3])),
    ?assert(check_sequence([1, 2, 3,   1, 2, 3, 4])),
    ?assert(check_sequence([1, 2, 3,   1, 2, 3, 4,   3, 4])),
    %% Invalid replays:
    ?assertError(_, check_sequence([1, 2, 3,   2])),
    ?assertError(_, check_sequence([1, 2, 3,   2, 4])),
    ?assertError(_, check_sequence([1, 2, 3,   2, 3, 4, 5,   3, 4])).
