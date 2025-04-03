%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022, 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_cluster_etcd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(ETCD_PORT, 2379).
-define(OPTIONS, [{server, ["http://127.0.0.1:" ++ integer_to_list(?ETCD_PORT)]},
                  {prefix, "emqxcl"},
                  {version, v2},
                  {node_ttl, 60}
                 ]).

all() -> ekka_ct:all(?MODULE).

init_per_testcase(t_restart_process, Config) ->
    case ekka_ct:is_tcp_server_available("localhost", ?ETCD_PORT) of
        true ->
            application:ensure_all_started(eetcd),
            Config;
        false ->
            {skip, no_etcd}
    end;
init_per_testcase(_TestCase, Config) ->
    ok = meck:new(ekka_httpc, [non_strict, no_history]),
    Config.

end_per_testcase(t_restart_process, _Config) ->
    application:stop(eetcd);
end_per_testcase(TestCase, _Config) ->
    ok = meck:unload(ekka_httpc),
    ekka_ct:cleanup(TestCase).

t_discover(_Config) ->
    Json = <<"{\"node\": {\"nodes\": [{\"key\": \"ekkacl/n1@127.0.0.1\"}]}}">>,
    ok = meck:expect(ekka_httpc, get, fun(_Server, _Path, _Params, _Opts) ->
                                              {ok, jsone:decode(Json)}
                                      end),
    {ok, ['n1@127.0.0.1']} = ekka_cluster_strategy:discover(ekka_cluster_etcd, ?OPTIONS).

t_lock(_Config) ->
    ok = meck:expect(ekka_httpc, put, fun(_Server, _Path, _Params, _Opts) ->
                                              {ok, jsone:decode(<<"{\"errorCode\": 0}">>)}
                                      end),
    ok = ekka_cluster_strategy:lock(ekka_cluster_etcd, ?OPTIONS).

t_unlock(_) ->
    ok = meck:expect(ekka_httpc, delete, fun(_Server, _Path, _Params, _Opts) ->
                                                 {ok, jsone:decode(<<"{\"errorCode\": 0}">>)}
                                         end),
    ok = ekka_cluster_strategy:unlock(ekka_cluster_etcd, ?OPTIONS).

t_register(_) ->
    ok = meck:new(ekka_cluster_sup, [non_strict, passthrough, no_history]),
    ok = meck:expect(ekka_cluster_sup, start_child, fun(_, _) -> {ok, self()} end),
    ok = meck:expect(ekka_httpc, put, fun(_Server, _Path, _Params, _Opts) ->
                                              {ok, jsone:decode(<<"{\"errorCode\": 0}">>)}
                                      end),
    ok = ekka_cluster_strategy:register(ekka_cluster_etcd, ?OPTIONS),
    ok = meck:unload(ekka_cluster_sup).

t_unregister(_) ->
    ok = meck:expect(ekka_httpc, delete, fun(_Server, _Path, _Params, _Opts) ->
                                                 {ok, jsone:decode(<<"{\"errorCode\": 0}">>)}
                                         end),
    ok = meck:expect(ekka_cluster_sup, stop_child, fun(_) -> ok end),
    ok = ekka_cluster_strategy:unregister(ekka_cluster_etcd, ?OPTIONS),
    ok = meck:unload(ekka_cluster_sup).

t_etcd_set_node_key(_) ->
    ok = meck:expect(ekka_httpc, put, fun(_Server, _Path, _Params, _Opts) ->
                                              {ok, jsone:decode(<<"{\"errorCode\": 0}">>)}
                                      end),
    {ok, #{<<"errorCode">> := 0}} = ekka_cluster_etcd:etcd_set_node_key(?OPTIONS).

t_restart_process(_) ->
    snabbkaffe:fix_ct_logging(),
    Options = lists:keyreplace(version, 1, ?OPTIONS, {version, v3}),
    Node = ekka_ct:start_slave(ekka, n1, [{ekka, cluster_discovery, {etcd, Options}}]),
    try
        ok = ekka_ct:wait_running(Node),
        Pid = erpc:call(Node, erlang, whereis, [ekka_cluster_etcd]),
        SupPid = erpc:call(Node, erlang, whereis, [ekka_sup]),
        Ref = monitor(process, Pid),
        SupRef = monitor(process, SupPid),
        exit(Pid, kill),
        receive
            {'DOWN', Ref, process, Pid, _} ->
                ok
        after
            200 -> exit(proc_not_killed)
        end,
        receive
            {'DOWN', SupRef, process, SupPid, _} ->
                exit(supervisor_died)
        after
            200 -> ok
        end,
        ok = ekka_ct:wait_running(Node, 2_000),
        ok
    after
        ok = ekka_ct:stop_slave(Node)
    end,
    ok.
