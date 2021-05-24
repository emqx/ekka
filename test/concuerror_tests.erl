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

-module(concuerror_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Check that waiting for shards with timeout=infinity always results in `ok'.
wait_for_shards_inf_test() ->
    {ok, Pid} = ekka_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch ekka_rlog_status:notify_shard_up(foo, node())
              end),
        spawn(fun() ->
                      catch ekka_rlog_status:notify_shard_up(bar, node())
              end),
        ?assertMatch(ok, ekka_rlog_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch(ok, ekka_rlog_status:wait_for_shards([foo, bar], infinity)),
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Check that waiting for shards with a finite timeout never hangs forever:
wait_for_shards_timeout_test() ->
    {ok, Pid} = ekka_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch ekka_rlog_status:notify_shard_up(foo, node())
              end),
        spawn(fun() ->
                      catch ekka_rlog_status:notify_shard_up(bar, node())
              end),
        Ret = ekka_rlog_status:wait_for_shards([foo, bar], 100),
        case Ret of
            ok ->
                %% It should always succeed the second time:
                ?assertMatch(ok, ekka_rlog_status:wait_for_shards([foo, bar], 100));
            {timeout, Shards} ->
                ?assertMatch([], Shards -- [foo, bar])
        end,
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

%% Check that waiting for events never results in infinite wait
wait_for_shards_crash_test() ->
    {ok, Pid} = ekka_rlog_status:start_link(),
    try
        spawn(fun() ->
                      catch ekka_rlog_status:notify_shard_up(foo, node())
              end),
        spawn(fun() ->
                      exit(Pid, shutdown)
              end),
        %% Check the result:
        try ekka_rlog_status:wait_for_shards([foo], 100) of
            ok ->
                %% It should always return `ok' the second time:
                ?assertMatch(ok, ekka_rlog_status:wait_for_shards([foo], 100));
            {timeout, _Shards} ->
                ok
        catch
            error:rlog_restarted -> ok
        end,
        ?assertMatch([], flush())
    after
        cleanup(Pid)
    end.

flush() ->
    receive
        A -> [A|flush()]
    after 100 ->
            []
    end.

cleanup(Pid) ->
    unlink(Pid),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end,
    ets:delete(ekka_rlog_replica_tab),
    ets:delete(ekka_rlog_stats_tab).
