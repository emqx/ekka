%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_autocluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DNS_OPTIONS, [{name, "localhost"},
                      {app, "ct"}
                     ]).

-define(ETCD_OPTIONS, [{server, ["http://127.0.0.1:2379"]},
                       {prefix, "cl"},
                       {node_ttl, 60}
                      ]).

-define(K8S_OPTIONS, [{apiserver, "http://127.0.0.1:6000"},
                      {namespace, "default"},
                      {service_name, "ekka"},
                      {address_type, ip},
                      {app_name, "ct"},
                      {suffix, ""}
                     ]).

-define(MCAST_OPTIONS, [{addr, {239,192,0,1}},
                        {ports, [5000,5001,5002]},
                        {iface, {0,0,0,0}},
                        {ttl, 255},
                        {loop, true}
                       ]).

suite() -> [{timetrap, {seconds, 60}}].

all() -> ekka_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    _ = inets:start(),
    ok = application:set_env(ekka, cluster_name, ekka),
    ok = application:set_env(ekka, cluster_enable, true),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped(),
    ok.

init_per_testcase(t_autocluster_retry_when_missing_nodes, Config) ->
    snabbkaffe:start_trace(),
    application:set_env(ekka, test_etcd_nodes,
                        ["n1@127.0.0.1", "ct@127.0.0.1", "n2@127.0.0.1"]),
    {ok, _} = start_etcd_server(2379),
    Nodes = [ekka_ct:start_slave(ekka, N) || N <- [n1, n2]],
    lists:foreach(
      fun(Node) ->
              ok = set_app_env(Node, {etcd, ?ETCD_OPTIONS}),
              ok = setup_cover(Node),
              ekka:leave()
      end,
      Nodes),
    ok = set_app_env(node(), {etcd, ?ETCD_OPTIONS}),
    [ {nodes, Nodes}
    | Config];
init_per_testcase(_TestCase, Config) ->
    ok = ekka:start(),
    Config.

end_per_testcase(t_autocluster_retry_when_missing_nodes, Config) ->
    [N1, N2] = ?config(nodes, Config),
    ekka:force_leave(N1),
    ekka:force_leave(N2),
    ok = ekka_ct:stop_slave(N1),
    ok = ekka_ct:stop_slave(N2),
    ok = stop_etcd_server(2379),
    application:unset_env(ekka, test_etcd_nodes),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    meck:unload(),
    ok.

%%--------------------------------------------------------------------
%% Autocluster via 'static' strategy

t_autocluster_via_static(_Config) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {static, [{seeds, [node()]}]}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.


t_autocluster_singleton(_Config) ->
    %% Verify that discovery of a single node completes immediately
    N1 = ekka_ct:start_slave(ekka, n1),
    ?check_trace(
       #{timetrap => 10000},
       try
           ok = ekka_ct:wait_running(N1),
           ok = set_app_env(N1, {static, [{seeds, [N1]}]}),
           ok = rpc:call(N1, ekka_autocluster, run, [ekka]),
           ?block_until(#{?snk_kind := ekka_autocluster_complete})
       after
           ok = ekka_ct:stop_slave(N1)
       end,
       []).

%%--------------------------------------------------------------------
%% Autocluster via 'dns' strategy

t_autocluster_via_dns(_Config) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {dns, ?DNS_OPTIONS}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

t_autocluster_via_dns_laggy(_Config) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    ?check_trace(
      #{timetrap => 15000},
      try
          ok = ekka_ct:wait_running(N1),
          ok = set_app_env(N1, {dns, [{name, "thishostnameshouldnotexist"},
                                      {app, "ct"}
                                     ]}),
          {ok, {ok, #{}}} = ?wait_async_action(
            rpc:call(N1, ekka, autocluster, []),
            #{?snk_kind := ekka_maybe_run_app_again}),
          ok = set_app_env(N1, {dns, ?DNS_OPTIONS}),
          ok = wait_for_node(N1),
          ok = ekka:force_leave(N1)
      after
          ok = ekka_ct:stop_slave(N1)
      end,
      []).

%%--------------------------------------------------------------------
%% Autocluster via 'etcd' strategy

t_autocluster_via_etcd(_Config) ->
    {ok, _} = start_etcd_server(2379),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {etcd, ?ETCD_OPTIONS}),
        _ = rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = stop_etcd_server(2379),
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% Autocluster via 'k8s' strategy

t_autocluster_via_k8s(_Config) ->
    {ok, _} = start_k8sapi_server(6000),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {k8s, ?K8S_OPTIONS}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = stop_k8sapi_server(6000),
        ok = ekka_ct:stop_slave(N1)
    end.

t_autocluster_retry_when_missing_nodes(Config) ->
    Nodes = [N1, N2] = ?config(nodes, Config),
    ThisNode = node(),
    assert_cluster_nodes_equal([N1], N1),
    assert_cluster_nodes_equal([N2], N2),
    assert_cluster_nodes_equal([ThisNode], ThisNode),

    %% Make the two nodes cluster with each other, but fail to
    %% cluster with master.  Check if autocluster is tried again.
    ct:pal("testing cluster with failure"),
    mock_autocluster_failure(Nodes),
    lists:foreach(
      fun(Node) ->
              ok = ekka_ct:wait_running(Node)
      end,
      Nodes),
    rpc:call(N1, ekka, autocluster, []),
    rpc:call(N2, ekka, autocluster, []),
    ekka:autocluster(),
    [{ok, _} = snabbkaffe:block_until(?match_n_events(2, #{?snk_kind := ekka_autocluster_loop,
                                                           ?snk_meta := #{node := N}}),
                                      _Timeout    = 10000,
                                      _BackInTIme = infinity)
     || N <- Nodes],
    assert_cluster_nodes_equal([N1, N2], N1),
    assert_cluster_nodes_equal([N1, N2], N2),
    assert_cluster_nodes_equal([ThisNode], ThisNode),
    %% Now remove the mock; the cluster should eventually heal
    %% itself.
    ok = rpc:call(N1, meck, unload, [ekka_node]),
    ok = rpc:call(N2, meck, unload, [ekka_node]),
    meck:unload(ekka_node),
    ct:pal("testing cluster without failure"),
    timer:sleep(10000),
    AllNodes = [ThisNode, N1, N2],
    assert_cluster_nodes_equal(AllNodes, N1),
    assert_cluster_nodes_equal(AllNodes, N2),
    assert_cluster_nodes_equal(AllNodes, ThisNode),
    ok.

%%--------------------------------------------------------------------
%% Autocluster via 'mcast' strategy

t_autocluster_via_mcast(_Config) ->
    ok = reboot_ekka_with_mcast_env(),
    ok = timer:sleep(1000),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {mcast, ?MCAST_OPTIONS}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

reboot_ekka_with_mcast_env() ->
    ok = ekka:stop(),
    ok = set_app_env(node(), {mcast, ?MCAST_OPTIONS}),
    ok = ekka:start().

t_autocluster_mcast_lock_failure(_Config) ->
    ok = reboot_ekka_with_mcast_env(),
    ok = timer:sleep(1000),
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {mcast, ?MCAST_OPTIONS}),
        assert_unlocked(ekka_cluster_mcast, N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

set_app_env(Node, Discovery) ->
    Config = [{ekka, [{cluster_name, ekka},
                      {cluster_enable, true},
                      {cluster_discovery, Discovery}
                     ]
              }],
    rpc:call(Node, application, set_env, [Config]).

wait_for_node(Node) ->
    wait_for_node(Node, 20).
wait_for_node(Node, Cnt) ->
    ok = timer:sleep(500),
    Running = ekka:info(running_nodes),
    case lists:member(Node, Running) of
        true -> timer:sleep(500), ok;
        false ->
            case Cnt > 0 of
                true -> wait_for_node(Node, Cnt-1);
                false -> error({autocluster_timeout, Node, Running})
            end
    end.

start_etcd_server(Port) ->
    start_http_server(Port, mod_etcd).

start_k8sapi_server(Port) ->
    start_http_server(Port, mod_k8s_api).

start_http_server(Port, Mod) ->
    Res = inets:start(httpd, [{port, Port},
                              {server_name, "etcd"},
                              {server_root, "."},
                              {document_root, "."},
                              {bind_address, "localhost"},
                              {modules, [Mod]}
                             ]),
    case Res of
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Err ->
            Err
    end.

stop_etcd_server(Port) ->
    stop_http_server(Port).

stop_k8sapi_server(Port) ->
    stop_http_server(Port).

stop_http_server(Port) ->
    inets:stop(httpd, {{127,0,0,1}, Port}).

setup_cover(Node) ->
    SrcPath = proplists:get_value(source, ekka_autocluster:module_info(compile)),
    CompileOpts = proplists:get_value(options, ekka_autocluster:module_info(compile)),
    {ok, _Pid} = rpc:call(Node, cover, start, []),
    {ok, _} = rpc:call(Node, cover, compile_module, [SrcPath, CompileOpts]),
    ok.

mock_autocluster_failure(Nodes) ->
    ThisNode = node(),
    lists:foreach(
      fun(Node) ->
              ok = rpc:call(Node, meck, new,
                            [ekka_node, [no_link, passthrough, no_history, non_strict]]),
              ok = rpc:call(Node, meck, expect,
                            [ekka_node, is_aliving,
                             fun(N) ->
                                     N =/= ThisNode
                             end])
      end,
      Nodes),
    ok = meck:new(ekka_node, [no_link, passthrough, no_history, non_strict]),
    ok = meck:expect(ekka_node, is_aliving,
                     fun(N) ->
                             N =:= ThisNode
                     end),
    ok.

assert_cluster_nodes_equal(ExpectedNodes0, Node) ->
    ct:pal("checking cluster nodes on ~p", [Node]),
    ExpectedNodes = lists:usort(ExpectedNodes0),
    Results0 = rpc:call(Node, ekka_mnesia, cluster_nodes, [all]),
    Results = lists:usort(Results0),
    ?assertEqual(ExpectedNodes, Results).

assert_unlocked(StrategyMod, Node) ->
    %% simulate a failure like timeout
    TestPid = self(),
    ok = rpc:call(Node, meck, new, [StrategyMod, [non_strict, passthrough,
                                                  no_history, no_link]]),
    ok = rpc:call(Node, meck, expect, [StrategyMod, lock,
                                       fun(_Options) -> error(timeout) end]),
    ok = rpc:call(Node, meck, expect, [StrategyMod, unlock,
                                       fun(_Options) ->
                                               TestPid ! unlocked,
                                               ok
                                       end]),
    _ = rpc:call(Node, ekka, autocluster, []),
    Timeout = 500,
    receive
        unlocked -> ok
    after
        Timeout -> error(failed_to_unlock)
    end,
    ok.
