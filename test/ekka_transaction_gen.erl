-module(ekka_transaction_gen).

-export([ create_keys/0
        ]).

-record(kv_tab, {key, val}).

create_keys() ->
    ekka_mnesia:transaction(
      fun() ->
              [mnesia:write(#kv_tab{ key = I
                                   , val = 0
                                   }) || I <- lists:seq(0, 99)]
      end).
