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
    %% TODO: ignoring the return type here, because some tests use CT
    %% master as a replica, and it doesn't have proper schema
    _ = ekka_mnesia:copy_table(test_tab, ram_copies).

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
