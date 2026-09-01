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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ETCD_PORT, 2379).
-define(OPTIONS, [{server, ["http://127.0.0.1:" ++ integer_to_list(?ETCD_PORT)]},
                  {prefix, "emqxcl"},
                  {version, v2},
                  {node_ttl, 60}
                 ]).

%% Tests that stop and start etcd run their own etcd in a docker
%% container, on a port of its own to not clash with a host etcd.
-define(V3_ETCD_PORT, 2479).
-define(V3_ETCD_CONTAINER, "ekka-ct-etcd").
-define(V3_ETCD_IMAGE, "gcr.io/etcd-development/etcd:v3.5.11").
-define(V3_OPTIONS, [{server, ["http://127.0.0.1:" ++ integer_to_list(?V3_ETCD_PORT)]},
                     {prefix, "emqxcl"},
                     {version, v3},
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
init_per_testcase(TestCase, Config) when TestCase =:= t_etcd_outage_recovery;
                                         TestCase =:= t_start_with_etcd_down ->
    case os:find_executable("docker") of
        false ->
            {skip, no_docker};
        _ ->
            Config
    end;
init_per_testcase(_TestCase, Config) ->
    ok = meck:new(ekka_httpc, [non_strict, no_history]),
    Config.

end_per_testcase(t_restart_process, _Config) ->
    application:stop(eetcd);
end_per_testcase(TestCase, _Config) when TestCase =:= t_etcd_outage_recovery;
                                         TestCase =:= t_start_with_etcd_down ->
    etcd_container_rm();
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

%% Regression test for emqx/emqx#12743: a transient etcd outage must
%% not disable cluster discovery permanently.
%%
%% Before the fix, the lease keepalive halt stopped the gen_server, the
%% restarted process crashed in init/1 while etcd was down, and the
%% crash loop exhausted the restart intensity of ekka_cluster_sup.
%% After etcd came back, the lease had expired, so the node key was
%% gone and nothing was left running to re-create it.
t_etcd_outage_recovery(_Config) ->
    snabbkaffe:fix_ct_logging(),
    ok = etcd_container_start(),
    ?check_trace(
       begin
           %% gen_rpc derives its listen port from the digits trailing
           %% the node name; pick numbers no other suite uses
           Node = ekka_ct:start_slave(ekka, netcd41,
                                      [{ekka, cluster_discovery, {etcd, ?V3_OPTIONS}}]),
           try
               ok = ekka_ct:wait_running(Node),
               ok = rpc:call(Node, ekka, autocluster, []),
               ok = wait_node_registered(Node, 30),
               SupPid = rpc_whereis(Node, ekka_cluster_sup),
               EtcdPid = rpc_whereis(Node, ekka_cluster_etcd),
               ?assert(is_pid(SupPid)),
               ?assert(is_pid(EtcdPid)),
               %% stop etcd; the lease keepalive halts
               ok = etcd_container_stop(),
               {ok, _} = ?block_until(#{?snk_kind := ekka_cluster_etcd_keepalive_halted},
                                      30_000),
               %% wait past the lease TTL: etcd deletes the node key
               ok = timer:sleep(7_000),
               %% the process and its supervisor survive the outage
               ?assertEqual(SupPid, rpc_whereis(Node, ekka_cluster_sup)),
               ?assertEqual(EtcdPid, rpc_whereis(Node, ekka_cluster_etcd)),
               ?assertEqual({error, etcd_disconnected},
                            rpc:call(Node, ekka_cluster_etcd, discover, [?V3_OPTIONS])),
               %% start etcd again; the node reconnects and re-registers
               ok = etcd_container_start(),
               {ok, _} = ?block_until(#{?snk_kind := ekka_cluster_etcd_reregistered},
                                      60_000),
               ok = wait_node_registered(Node, 30),
               ?assertEqual(SupPid, rpc_whereis(Node, ekka_cluster_sup)),
               ?assertEqual(EtcdPid, rpc_whereis(Node, ekka_cluster_etcd))
           after
               ok = ekka_ct:stop_slave(Node)
           end
       end,
       fun(Trace) ->
           %% the reconnect attempts back off instead of spinning
           NAttempts = length(?of_kind(ekka_cluster_etcd_reconnect_failed, Trace)),
           ?assert(NAttempts >= 1),
           ?assert(NAttempts =< 10)
       end).

%% ekka must boot with etcd unreachable and join the cluster once etcd
%% becomes available.
t_start_with_etcd_down(_Config) ->
    snabbkaffe:fix_ct_logging(),
    ok = etcd_container_rm(),
    Node = ekka_ct:start_slave(ekka, netcd42,
                               [{ekka, cluster_discovery, {etcd, ?V3_OPTIONS}}]),
    try
        ok = ekka_ct:wait_running(Node),
        ?assert(is_pid(rpc_whereis(Node, ekka_cluster_sup))),
        ?assert(is_pid(rpc_whereis(Node, ekka_cluster_etcd))),
        ?assertEqual({error, etcd_disconnected},
                     rpc:call(Node, ekka_cluster_etcd, discover, [?V3_OPTIONS])),
        %% the autocluster discovery loop keeps retrying while etcd is
        %% unreachable
        ok = rpc:call(Node, ekka, autocluster, []),
        ok = etcd_container_start(),
        ok = wait_node_registered(Node, 60)
    after
        ok = ekka_ct:stop_slave(Node)
    end.

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

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

rpc_whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

wait_node_registered(Node, 0) ->
    error({timeout_waiting_for_registration, Node});
wait_node_registered(Node, Retries) ->
    case rpc:call(Node, ekka_cluster_etcd, discover, [?V3_OPTIONS]) of
        {ok, Nodes} ->
            case lists:member(Node, Nodes) of
                true ->
                    ok;
                false ->
                    ok = timer:sleep(1000),
                    wait_node_registered(Node, Retries - 1)
            end;
        _Other ->
            ok = timer:sleep(1000),
            wait_node_registered(Node, Retries - 1)
    end.

etcd_container_start() ->
    Cmd = case etcd_container_exists() of
              true ->
                  "docker start " ++ ?V3_ETCD_CONTAINER;
              false ->
                  "docker run -d --name " ++ ?V3_ETCD_CONTAINER
                  ++ " -p " ++ integer_to_list(?V3_ETCD_PORT) ++ ":2379 "
                  ++ ?V3_ETCD_IMAGE
                  ++ " /usr/local/bin/etcd"
                  ++ " --listen-client-urls http://0.0.0.0:2379"
                  ++ " --advertise-client-urls http://0.0.0.0:2379"
          end,
    _ = os:cmd(Cmd),
    wait_etcd_ready(60).

etcd_container_stop() ->
    _ = os:cmd("docker stop " ++ ?V3_ETCD_CONTAINER),
    ok.

etcd_container_rm() ->
    _ = os:cmd("docker rm -f " ++ ?V3_ETCD_CONTAINER),
    ok.

etcd_container_exists() ->
    Out = os:cmd("docker ps -a --filter name='^" ++ ?V3_ETCD_CONTAINER
                 ++ "$' --format '{{.Names}}'"),
    string:find(Out, ?V3_ETCD_CONTAINER) =/= nomatch.

wait_etcd_ready(0) ->
    {error, etcd_not_ready};
wait_etcd_ready(Retries) ->
    case ekka_ct:is_tcp_server_available("127.0.0.1", ?V3_ETCD_PORT) of
        true ->
            ok;
        false ->
            ok = timer:sleep(1000),
            wait_etcd_ready(Retries - 1)
    end.
