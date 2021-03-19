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

-export([ init/0
        , delete/1
        , mnesia/1
        , benchmark/3
        ]).

-record(test_tab, {key, val}).

mnesia(boot) ->
    case application:get_env(ekka, test_tabs, false) of
        true ->
            ok = ekka_mnesia:create_table(test_tab, [{type, ordered_set},
                                                     {ram_copies, [node()]},
                                                     {record_name, test_tab},
                                                     {attributes, record_info(fields, test_tab)}
                                                    ]);
        false ->
            ok
    end;
mnesia(copy) ->
    case application:get_env(ekka, test_tabs, false) of
        true ->
            ok = ekka_mnesia:copy_table(test_tab, ram_copies);
        false ->
            ok
    end.

init() ->
    ekka_mnesia:transaction(
      fun() ->
              [mnesia:write(#test_tab{ key = I
                                     , val = 0
                                     }) || I <- lists:seq(0, 4)]
      end).

delete(K) ->
    ekka_mnesia:transaction(
      fun() ->
              mnesia:delete({test_tab, K})
      end).

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
