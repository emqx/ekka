%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(ekka_test_table).

-export([init/0, insert/2]).

-record(test_table, {key, val}).

init() ->
    ok = ekka_mnesia:create_table(test_table, [
            {type, set}, {disc_copies, [node()]},
            {record_name, test_table},
            {attributes, record_info(fields, test_table)}]),
    ok = ekka_mnesia:copy_table(test_table, disc_copies).

insert(Key, Val) ->
    mnesia:transaction(fun mnesia:write/1, [#test_table{key = Key, val = Val}]).

