-ifndef(EKKA_RLOG_HRL).
-define(EKKA_RLOG_HRL, true).

-record(rlog,
        { key :: ekka_rlog_lib:txid()
        , ops :: ekka_rlog_lib:tx()
        }).

-endif.
