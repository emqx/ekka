-ifndef(EKKA_RLOG_HRL).
-define(EKKA_RLOG_HRL, true).

-record(rlog,
        { key :: ekka_rlog_lib:txid()
        , ops :: [ekka_rlog_lib:op()]
        }).

%% Tables and table keys:
-define(replica_tab, ekka_rlog_replica_tab).
-define(upstream_node, upstream_node).

-endif.
