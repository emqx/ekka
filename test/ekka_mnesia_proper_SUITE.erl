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
-module(ekka_mnesia_proper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").

%%================================================================================
%% Types
%%================================================================================

-type key() :: proper:range(1, 20).
-type value() :: proper:range(1, 50).

-record(s,
        { bag = []  :: [{key, value()}]
        , set = #{} :: #{key() => value()}
        }).

%%================================================================================
%% Testcases
%%================================================================================

t_import_transactions(Config0) when is_list(Config0) ->
    Config = [{proper, #{max_size => 300,
                         numtests => 100,
                         timeout  => 100000
                        }} | Config0],
    ?run_prop(Config, prop()).

prop() ->
    Cluster = ekka_ct:cluster([core, replicant], ekka_mnesia_test_util:common_env()),
    ?forall_trace(
       Cmds, commands(?MODULE),
       try
           Nodes = ekka_ct:start_cluster(ekka, Cluster),
           ok = ekka_mnesia_test_util:wait_shards(Nodes),
           {History, State, Result} = run_commands(?MODULE, Cmds),
           ekka_mnesia_test_util:stabilize(100),
           [check_state(Cmds, State, Node) || Node <- Nodes],
           {History, State, Result}
       after
           ekka_ct:teardown_cluster(Cluster)
       end,
       fun({History, State, Result}, Trace) ->
               ?assertMatch(ok, Result),
               true
       end).

%%================================================================================
%% Proper generators
%%================================================================================

table_key() ->
    non_neg_integer().

value() ->
    non_neg_integer().

table() ->
    union([test_tab, test_bag]).

write_op(Table) ->
    {write, Table, table_key(), value()}.

trans_op(#s{bag = Bag, set = Set}) ->
    ?LET(Table, table(),
         case Table of
             test_tab ->
                 case maps:keys(Set) of
                     [] ->
                         write_op(Table);
                     Keys ->
                         frequency([ {60, write_op(Table)}
                                   , {20, {delete, Table, oneof(Keys)}}
                                   ])
                 end;
             test_bag ->
                 case Bag of
                     [] ->
                         write_op(Table);
                     Objs ->
                         Keys = proplists:get_keys(Objs),
                         frequency([ {60, write_op(Table)}
                                   , {10, {delete, Table, oneof(Keys)}}
                                   , {30, {delete_object, Table, oneof(Objs)}}
                                   ])
                 end
         end).

transaction(State) ->
    frequency([ {95, {transaction, resize(10, list(trans_op(State)))}}
              , {5, {clear_table, table()}}
              ]).

participant() ->
    oneof([core_node(), replicant_node()]).

%%================================================================================
%% Proper FSM definition
%%================================================================================

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    #s{}.

command(State) ->
    frequency([ {90, {call, ?MODULE, execute, [participant(), transaction(State)]}}
              , {10, {call, ?MODULE, restart_replicant, []}}
              ]).

%% Picks whether a command should be valid under the current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
    true.

postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

next_state(State, _Res, {call, ?MODULE, execute, [_, Args]}) ->
    case Args of
        {clear_table, test_tab} ->
            State#s{set = #{}};
        {clear_table, test_bag} ->
            State#s{bag = []};
        {transaction, Ops} ->
            lists:foldl(fun symbolic_exec_trans_op/2, State, Ops)
    end;
next_state(State, _Res, _Call) ->
    State.

check_state(Cmds, #s{bag = Bag, set = Set}, Node) ->
    compare_lists(bag, Node, Cmds, lists:sort(Bag), get_records(Node, test_bag)),
    compare_lists(set, Node, Cmds, lists:sort(maps:to_list(Set)), get_records(Node, test_tab)).

compare_lists(Type, Node, Cmds, Expected, Got) ->
    Unexpected = Expected -- Got,
    Missing = Got -- Expected,
    Comment = [ {node, Node}
              , {cmds, Cmds}
              , {unexpected, Unexpected}
              , {missing, Missing}
              , {table_type, Type}
              ],
    ?assert(length(Missing) + length(Unexpected) =:= 0, Comment).

%%================================================================================
%% Internal functions
%%================================================================================

symbolic_exec_trans_op({write, test_tab, Key, Val}, State = #s{set = Old}) ->
    Set = Old#{Key => Val},
    State#s{set = Set};
symbolic_exec_trans_op({write, test_bag, Key, Val}, State = #s{bag = Old}) ->
    Rec = {Key, Val},
    Bag = [Rec | Old -- [Rec]],
    State#s{bag = Bag};
symbolic_exec_trans_op({delete, test_tab, Key}, State = #s{set = Old}) ->
    Set = maps:remove(Key, Old),
    State#s{set = Set};
symbolic_exec_trans_op({delete, test_bag, Key}, State = #s{bag = Old}) ->
    Bag = proplists:delete(Key, Old),
    State#s{bag = Bag};
symbolic_exec_trans_op({delete_object, test_bag, Rec}, State = #s{bag = Old}) ->
    Bag = lists:delete(Rec, Old),
    State#s{bag = Bag}.

execute_op({write, Tab, Key, Val}) ->
    ok = mnesia:write({Tab, Key, Val});
execute_op({delete, Tab, Key}) ->
    ok = mnesia:delete({Tab, Key});
execute_op({delete_object, Tab, {K, V}}) ->
    ok = mnesia:delete_object({Tab, K, V}).

execute(Node, {clear_table, Tab}) ->
    {atomic, ok} = rpc:call(Node, ekka_mnesia, clear_table, [Tab]);
execute(Node, {transaction, Ops}) ->
    Fun = fun() ->
                  lists:foreach(fun execute_op/1, Ops)
          end,
    {atomic, ok} = rpc:call(Node, ekka_mnesia, transaction, [test_shard, Fun]).

restart_replicant() ->
    ok = rpc:call(replicant_node(), application, stop, [ekka]),
    ok = rpc:call(replicant_node(), application, start, [ekka]).

core_node() ->
    ekka_ct:node_id(n1).

replicant_node() ->
    ekka_ct:node_id(n2).

get_records(Node, Table) ->
    Records = rpc:call(Node, ets, tab2list, [Table]),
    lists:sort([{K, V} || {_, K, V} <- Records]).
