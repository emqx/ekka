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

%% @doc Functions for accessing the RLOG configuration
-module(ekka_rlog_config).

-export([ init/0

        , role/0
        , backend/0
        , rpc_module/0

          %% Shard config:
        , shard_rlookup/1
        , shards/0
        , shard_config/1
        ]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type raw_config() :: [{ekka_rlog:shard(), [ekka_rlog_lib:table()]}].

%%================================================================================
%% Persistent term keys
%%================================================================================

-define(shard_rlookup(TABLE), {ekka_shard_rlookup, TABLE}).
-define(shard_config(SHARD), {ekka_shard_config, SHARD}).
-define(shards, ekka_shards).

-define(ekka(Key), {ekka, Key}).

%%================================================================================
%% API
%%================================================================================

%% @doc Find which shard the table belongs to
-spec shard_rlookup(ekka_rlog_lib:table()) -> ekka_rlog:shard() | undefined.
shard_rlookup(Table) ->
    persistent_term:get(?shard_rlookup(Table), undefined).

%% @doc List shards
-spec shards() -> [ekka_rlog:shard()].
shards() ->
    persistent_term:get(?shards, []).

-spec shard_config(ekka_rlog:shard()) -> ekka_rlog:shard_config().
shard_config(Shard) ->
    persistent_term:get(?shard_config(Shard)).

-spec backend() -> ekka_mnesia:backend().
backend() ->
    persistent_term:get(?ekka(db_backend), mnesia).

-spec role() -> ekka_rlog:role().
role() ->
    persistent_term:get(?ekka(node_role), core).

-spec rpc_module() -> gen_rpc | rpc.
rpc_module() ->
    persistent_term:get(?ekka(rlog_rpc_module), gen_rpc).

-spec init() -> ok.
init() ->
    copy_from_env(rlog_rpc_module),
    copy_from_env(db_backend),
    copy_from_env(node_role),
    load_shard_config().

%%================================================================================
%% Internal
%%================================================================================

-spec copy_from_env(atom()) -> ok.
copy_from_env(Key) ->
    case application:get_env(ekka, Key) of
        {ok, Val} -> persistent_term:put(?ekka(Key), Val);
        undefined -> ok
    end.

-spec load_shard_config() -> ok.
load_shard_config() ->
    load_shard_config(read_shard_config()).

-spec load_shard_config(raw_config()) -> ok.
load_shard_config(Raw) ->
    ok = verify_shard_config(Raw),
    erase_shard_config(),
    create_shard_rlookup(Raw),
    Shards = proplists:get_keys(Raw),
    ok = persistent_term:put(?shards, Shards),
    lists:foreach(fun({Shard, Tables}) ->
                          Config = #{ tables => Tables
                                    , match_spec => make_shard_match_spec(Tables)
                                    },
                          ?tp(notice, "Setting RLOG shard config",
                              #{ shard => Shard
                               , tables => Tables
                               }),
                          ok = persistent_term:put(?shard_config(Shard), Config)
                  end,
                  Raw).

-spec verify_shard_config(raw_config()) -> ok.
verify_shard_config(ShardConfig) ->
    verify_shard_config(ShardConfig, #{}).

-spec verify_shard_config(raw_config(), #{ekka_rlog_lib:table() => ekka_rlog:shard()}) -> ok.
verify_shard_config([], _) ->
    ok;
verify_shard_config([{_Shard, []} | Rest], Acc) ->
    verify_shard_config(Rest, Acc);
verify_shard_config([{Shard, [Table|Tables]} | Rest], Acc0) ->
    Acc = case Acc0 of
              #{Table := Shard1} when Shard1 =/= Shard ->
                  ?tp(critical, "Duplicate RLOG shard",
                      #{ table       => Table
                       , shard       => Shard
                       , other_shard => Shard1
                       }),
                  error(badarg);
              _ ->
                  Acc0#{Table => Shard}
          end,
    verify_shard_config([{Shard, Tables} | Rest], Acc).

-spec read_shard_config() -> [{ekka_rlog:shard(), [ekka_rlog_lib:table()]}].
read_shard_config() ->
    L = lists:flatmap( fun({_App, _Module, Attrs}) ->
                               Attrs
                       end
                     , ekka_boot:all_module_attributes(rlog_shard)
                     ),
    Shards = proplists:get_keys(L),
    [{Shard, lists:usort(proplists:get_all_values(Shard, L))}
     || Shard <- Shards].

%% Create a reverse lookup table for finding shard of the table
-spec create_shard_rlookup([{ekka_rlog:shard(), [ekka_rlog_lib:table()]}]) -> ok.
create_shard_rlookup(Shards) ->
    lists:foreach(
      fun({Shard, Tables}) ->
              [persistent_term:put(?shard_rlookup(Tab), Shard) || Tab <- Tables]
      end, Shards).

%% Delete the persistent terms created by us
-spec erase_shard_config() -> ok.
erase_shard_config() ->
    lists:foreach( fun({Key, _}) ->
                           case Key of
                               ?shard_rlookup(_) ->
                                   persistent_term:erase(Key);
                               ?shard_config(_) ->
                                   persistent_term:erase(Key);
                               ?shards ->
                                   persistent_term:erase(Key);
                               _ ->
                                   ok
                           end
                   end
                 , persistent_term:get()
                 ).

-spec make_shard_match_spec([ekka_rlog_lib:table()]) -> ets:match_spec().
make_shard_match_spec(Tables) ->
    [{ {{Table, '_'}, '_', '_'}
     , []
     , ['$_']
     } || Table <- Tables].

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-dialyzer({nowarn_function, verify_shard_config_test/0}).
verify_shard_config_test() ->
    ?assertMatch(ok, verify_shard_config([])),
    ?assertMatch(ok, verify_shard_config([ {foo, [foo_tab, bar_tab]}
                                         , {baz, [baz_tab, foo_bar_tab]}
                                         ])),
    ?assertError(_, verify_shard_config([ {foo, [foo_tab, bar_tab]}
                                        , {baz, [baz_tab, foo_tab]}
                                        ])).

shard_rlookup_test() ->
    PersTerms = lists:sort(persistent_term:get()),
    try
        ok = load_shard_config([ {foo, [foo_tab1, foo_tab2]}
                               , {bar, [bar_tab1, bar_tab2]}
                               ]),
        ?assertMatch(foo, shard_rlookup(foo_tab1)),
        ?assertMatch(foo, shard_rlookup(foo_tab2)),
        ?assertMatch(bar, shard_rlookup(bar_tab1)),
        ?assertMatch(bar, shard_rlookup(bar_tab2))
    after
        erase_shard_config(),
        %% Check that erase_shard_config function restores the status quo:
        ?assertEqual(PersTerms, lists:sort(persistent_term:get()))
    end.

-endif. %% TEST
