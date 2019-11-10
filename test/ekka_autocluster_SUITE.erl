%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> ekka_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    ok = application:set_env(ekka, cluster_name, ekka),
    ok = application:set_env(ekka, cluster_enable, true),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% Autocluster via 'static' strategy

xt_autocluster_via_static(Config) ->
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

%%--------------------------------------------------------------------
%% Autocluster via 'dns' strategy

xt_autocluster_via_dns(Config) ->
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        Options = [{name, "localhost"}, {app, "ct"}],
        ok = set_app_env(N1, {dns, Options}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% Autocluster via 'etcd' strategy

t_autocluster_via_etcd(Config) ->
    Options = [{server, "http://127.0.0.1:2379"},
               {prefix, "cl"},
               {node_ttl, 60}
              ],
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {etcd, Options}),
        ok = rpc:call(N1, ?MODULE, autocluster_via_etc, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

autocluster_via_etc() ->
    {ok, _} = application:ensure_all_started(meck),
    ok = meck:new(httpc, [non_strict, passthrough, no_history]),
    Json = <<"{\"node\": {\"nodes\": [{\"key\": \"cl/ct@127.0.0.1\"}]}}">>,
    ok = meck:expect(httpc, request,
                     fun(get, Req, _Opts, _) ->
                             io:format("!!!!!!Get ~p~n", [Req]),
                             {ok, {{"HTTP/1.1", 200, "OK"}, [], Json}};
                        (Method, Req, _Opts, _) ->
                             io:format("!!!!!!~s ~p~n", [Method, Req]),
                             {ok, 200, <<"{\"errorCode\": 0}">>}
                     end),
    ekka:autocluster(),
    ok = meck:unload(httpc).

%%--------------------------------------------------------------------
%% Autocluster via 'k8s' strategy

xt_autocluster_via_k8s(Config) ->
    Options = [{apiserver, "http://127.0.0.1:8080"},
               {namespace, "default"},
               {service_name, "ekka"},
               {address_type, ip},
               {app_name, "ct"},
               {suffix, ""}],
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {k8s, Options}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% Autocluster via 'mcast' strategy

xt_autocluster_via_mcast(Config) ->
    Options = [{addr, {239,192,0,1}},
               {ports, [4369,4370]},
               {iface, {0,0,0,0}},
               {ttl, 255},
               {loop, true}
              ],
    N1 = ekka_ct:start_slave(ekka, n1),
    try
        ok = ekka_ct:wait_running(N1),
        ok = set_app_env(N1, {mcast, Options}),
        rpc:call(N1, ekka, autocluster, []),
        ok = wait_for_node(N1),
        ok = ekka:force_leave(N1)
    after
        ok = ekka_ct:stop_slave(N1)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

set_app_env(Node, Discovery) ->
    Envs = [{cluster_name, ekka},
            {cluster_enable, true},
            {cluster_discovery, Discovery}
           ],
    lists:foreach(
      fun({Par, Val}) ->
              rpc:call(Node, application, set_env, [ekka, Par, Val])
      end, Envs).

wait_for_node(Node) ->
    wait_for_node(Node, 10).
wait_for_node(Node, 0) ->
    error({autocluster_timeout, Node});
wait_for_node(Node, Cnt) ->
    ok = timer:sleep(500),
    case lists:member(Node, ekka:info(running_nodes)) of
        true -> ok;
        false -> wait_for_node(Node, Cnt-1)
    end.

