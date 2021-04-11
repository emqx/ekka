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

%% This module contains helper functions that run inside ekka_rlog:transaction
-module(ekka_rlog_activity).

-include("ekka.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("mnesia/src/mnesia.hrl").

-export([ transaction/1
        , transaction/2
        , ro_transaction/1
        ]).

%%================================================================================
%% API functions
%%================================================================================

-spec transaction(fun(() -> A)) -> A.
transaction(Fun) ->
    {atomic, Ret} = mnesia:transaction(Fun),
    Ret.

-spec transaction(fun((...) -> A), list()) -> A.
transaction(Fun, Args) ->
    {atomic, Ret} = mnesia:transaction(Fun, Args),
    Ret.

-spec ro_transaction(fun(() -> A)) -> A.
ro_transaction(Fun) ->
    {atomic, Ret} = mnesia:transaction(Fun),
    assert_ro(),
    Ret.

%%================================================================================
%% Internal functions
%%================================================================================

assert_ro() ->
    %% Only run this check in TEST mode
    {_, _, #tidstore{store = Ets}} = mnesia:get_activity_id(),
    case ets:match(Ets, {'_', '_', '_'}) of
        []  -> ok;
        Ops -> error({transaction_is_not_readonly, Ops})
    end.
