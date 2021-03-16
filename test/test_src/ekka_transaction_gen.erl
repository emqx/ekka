-module(ekka_transaction_gen).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ init/0
        , delete/1
        , mnesia/1
        ]).

-record(test_tab, {key, val}).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(test_tab, [
                {type, ordered_set},
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

delete(K) ->
    ekka_mnesia:transaction(
      fun() ->
              mnesia:delete({test_tab, K})
      end).
