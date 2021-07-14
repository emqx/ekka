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

%% Functions related to the management of the RLOG schema
-module(ekka_rlog_schema).

%% API:
-export([mnesia/1, add_table/2, tables_of_shard/1]).


-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-include("ekka_rlog.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API
%%================================================================================

%% @doc Add a table to the shard
%%
%% Note: currently it's the only schema operation that we support. No
%% removal and no handover of the table between the shards is
%% possible.
%%
%% These operations are too rare and expensive to implement, because
%% they require precise coordination of the shard processes across the
%% entire cluster.
%%
%% Adding an API to remove or modify schema would open possibility to
%% move a table from one shard to another. This requires restarting
%% both shards in a synchronized manner to avoid a race condition when
%% the replicant processes from the old shard import in-flight
%% transactions while the new shard is bootstrapping the table.
%%
%% This is further complicated by the fact that the replicant nodes
%% may consume shard transactions from different core nodes.
%%
%% So the operation of removing a table from the shard would look like
%% this:
%%
%% 1. Do an RPC call to all core nodes to stop the shard
%% 2. Each core node synchronously stops all the attached replicant
%%    processes
%% 3. Only then we are sure that we can avoid data corruption
%%
%% Currently there is no requirement to implement this, so we can get
%% away with managing each shard separately
-spec add_table(ekka_rlog:shard(), ekka_mnesia:table()) -> ok.
add_table(Shard, Table) ->
    case has_schema() of
        false ->
            ok;
        true ->
            case mnesia:transaction(fun do_add_table/2, [Shard, Table], infinity) of
                {atomic, ok}   -> ok;
                {aborted, Err} -> error({bad_schema, Shard, Table, Err})
            end
    end.

%% @doc Create the internal schema table if needed
mnesia(StartType) ->
    case has_schema() of
        true -> do_mnesia(StartType);
        _    -> ok
    end.

%% @private Return the list of tables that belong to the shard.
-spec tables_of_shard(ekka_rlog:shard()) -> [ekka_mnesia:table()].
tables_of_shard(Shard) ->
    true = has_schema(),
    {atomic, Tables} =
        mnesia:transaction(
          fun() ->
                  mnesia:match_object(#?schema{mnesia_table = '$1', shard = Shard})
          end),
    lists:flatten(Tables).

%%================================================================================
%% Internal functions
%%================================================================================

-spec has_schema() -> boolean().
has_schema() ->
    %% Note: replicant nodes don't have their own copies of the schema
    %% table. All the data is transferred to the replicant in the
    %% handshake message
    case {ekka_rlog_config:backend(), ekka_rlog_config:role()} of
        {rlog, core} -> true;
        _            -> false
    end.

do_mnesia(boot) ->
    ok = ekka_mnesia:create_table_internal(?schema, [{type, ordered_set},
                                                     {ram_copies, [node()]},
                                                     {record_name, ?schema},
                                                     {attributes, record_info(fields, ?schema)}
                                                    ]);
do_mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?schema, ram_copies).

-spec do_add_table(ekka_rlog:shard(), ekka_mnesia:table()) -> ok.
do_add_table(Shard, Table) ->
    case mnesia:wread({?schema, Table}) of
        [] ->
            mnesia:write(#?schema{ mnesia_table = Table
                                 , shard = Shard
                                 }),
            ok;
        [#?schema{shard = Shard}] ->
            %% We're just being idempotent here:
            ok;
        _ ->
            error(bad_schema)
    end.
