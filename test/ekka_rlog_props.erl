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
        , counter_import_check/2
        ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

replicant_bootstrap_stages(Node, Trace0) ->
    Trace = ?of_domain([ekka, rlog, replica|_], Trace0),
    ?causality(# {?snk_kind := state_change, to := disconnected, ?snk_meta := #{pid := _Pid}}
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

counter_import_check(CounterKey, Trace) ->
    Writes = [element(3, Rec) || #{ ?snk_kind := rlog_import_trans
                                  , ops := [{{test_tab, K}, Rec, write}]
                                  } <- Trace, K =:= CounterKey],
    ?assert(length(Writes) > 0),
    ok = check_sequence(Writes).

check_sequence([First|Rest]) ->
    check_sequence(First, Rest).

check_sequence(Pred, []) ->
    ok;
check_sequence(Pred, [Next|Rest]) ->
    ?assertEqual(Next, Pred + 1),
    check_sequence(Next, Rest).
