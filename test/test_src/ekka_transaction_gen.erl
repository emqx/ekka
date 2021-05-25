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
-module(ekka_transaction_gen).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).
-rlog_shard({test_shard, test_tab}).

-export([ init/0
        , delete/1
        , abort/2
        , mnesia/1
        , benchmark/3
        , counter/2
        , counter/3
        , ro_read_all_keys/0
        ]).

-record(test_tab, {key, val}).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(test_tab, [{type, ordered_set},
                                             {ram_copies, [node()]},
                                             {record_name, test_tab},
                                             {attributes, record_info(fields, test_tab)}
                                            ]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(test_tab, ram_copies).

init() ->
    ekka_mnesia:transaction(
      fun() ->
              [mnesia:write(#test_tab{ key = I
                                     , val = 0
                                     }) || I <- lists:seq(0, 4)]
      end).

ro_read_all_keys() ->
    ekka_mnesia:ro_transaction(
      test_shard,
      fun() ->
              Keys = mnesia:all_keys(test_tab),
              [ekka_ct:read(test_tab, K) || K <- Keys]
      end).

delete(K) ->
    ekka_mnesia:transaction(
      fun() ->
              mnesia:delete({test_tab, K})
      end).

counter(Key, N) ->
    counter(Key, N, 0).

counter(_Key, 0, _) ->
    ok;
counter(Key, NIter, Delay) ->
    {atomic, Val} =
        ekka_mnesia:transaction(
          fun() ->
                  case ekka_ct:read(test_tab, Key) of
                      [] -> V = 0;
                      [#test_tab{val = V}] -> V
                  end,
                  ok = ekka_ct:write(#test_tab{key = Key, val = V + 1}),
                  V
          end),
    ?tp(info, trans_gen_counter_update,
        #{ key => Key
         , value => Val
         }),
    timer:sleep(Delay),
    counter(Key, NIter - 1, Delay).

abort(Backend, AbortKind) ->
    Backend:transaction(
      fun() ->
              mnesia:write(#test_tab{key = canary_key, val = canary_dead}),
              do_abort(AbortKind)
      end).

do_abort(abort) ->
    mnesia:abort(deliberate);
do_abort(error) ->
    error(deliberate);
do_abort(exit) ->
    exit(deliberate);
do_abort(throw) ->
    throw(deliberate).

benchmark(ResultFile,
          #{ delays := Delays
           , backend := Backend
           , trans_size := NKeys
           , max_time := MaxTime
           }, NNodes) ->
    NReplicas = length(mnesia:table_info(test_tab, ram_copies)),
    case Backend of
        ekka_mnesia ->
            true = NReplicas =< 2;
        mnesia ->
            NNodes = NReplicas
    end,
    TransTimes =
        [begin
             ekka_ct:set_network_delay(Delay),
             do_benchmark(Backend, NKeys, MaxTime)
         end
         || Delay <- Delays],
    [snabbkaffe:push_stat({Backend, Delay}, NNodes, T)
     || {Delay, T} <- lists:zip(Delays, TransTimes)],
    ok = file:write_file( ResultFile
                        , ekka_ct:vals_to_csv([NNodes | TransTimes])
                        , [append]
                        ).

do_benchmark(Backend, NKeys, MaxTime) ->
    {T, NTrans} = timer:tc(fun() ->
                                   timer:send_after(MaxTime, complete),
                                   loop(0, Backend, NKeys)
                           end),
    T / NTrans.

loop(Cnt, Backend, NKeys) ->
    receive
        complete -> Cnt
    after 0 ->
            {atomic, _} = Backend:transaction(
                            fun() ->
                                    [begin
                                         mnesia:read({test_tab, Key}),
                                         mnesia:write(#test_tab{key = Key, val = Cnt})
                                     end || Key <- lists:seq(1, NKeys)]
                            end),
            loop(Cnt + 1, Backend, NKeys)
    end.
