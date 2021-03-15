-module(ekka_transaction_gen).

-export([ init/0
        ]).

-record(kv_tab, {key, val}).

init() ->
    %% TODO: do it in a nicer way
    ok = ekka_mnesia:create_table(kv_tab, [{ram_copies, ekka_rlog:core_nodes()},
                                           {record_name, kv_tab},
                                           {attributes, record_info(fields, kv_tab)},
                                           {storage_properties, []}]),
    mnesia:wait_for_tables([kv_tab], 10000),
    ekka_mnesia:transaction(
      fun() ->
              [mnesia:write(#kv_tab{ key = I
                                   , val = 0
                                   }) || I <- lists:seq(0, 4)]
      end).
