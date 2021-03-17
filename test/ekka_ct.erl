%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_ct).

-compile(export_all).
-compile(nowarn_export_all).

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
    lists:usort([F || {F, 1} <- Suite:module_info(exports),
                      string:substr(atom_to_list(F), 1, 2) == "t_"
                ]).

-spec cluster([{core | replicant, Node}], [{atom(), term()}]) -> Node
              when Node :: atom().
cluster(ClusterSpec, Env) ->
    %% Set common environment variables:
    CoreNodes = [node_id(Name) || {core, Name} <- ClusterSpec],
    Env1 = [{ekka, core_nodes, CoreNodes} | Env],
    %% Start nodes:
    start_core_nodes([N || {core, N} <- ClusterSpec], Env1),
    start_replicant_nodes([N || {replicant, N} <- ClusterSpec], Env1),
    [node_id(N) || {_, N} <- ClusterSpec].

start_core_nodes([], _Env) ->
    ok;
start_core_nodes([First|Rest], Env) ->
    Env1 = [{ekka, node_role, core} | Env],
    N1 = start_slave(ekka, First, Env1),
    [begin
         Node = start_slave(ekka, Name, Env1),
         ok = rpc:call(Node, ekka, join, [N1])
     end
     || Name <- Rest].

start_replicant_nodes(Nodes, Env) ->
    Env1 = [{ekka, node_role, replicant} | Env],
    [start_slave(ekka, N, Env1) || N <- Nodes].

start_slave(NodeOrEkka, Name) ->
    start_slave(NodeOrEkka, Name, []).

start_slave(node, Name, Env) ->
    {ok, Node} = slave:start(host(), Name, ebin_path()),
    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during ekka start:
    [rpc:call(Node, application, load, [App]) || App <- [gen_rpc, ekka]],
    %% Disable gen_rpc listener by default:
    Env1 = [{gen_rpc, tcp_server_port, false}|Env],
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env1],
    ok = snabbkaffe:forward_trace(Node),
    Node;
start_slave(ekka, Name, Env) ->
    Node = start_slave(node, Name, Env),
    ok = rpc:call(Node, ekka, start, []),
    Node.

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc:call(Node, ekka, is_running, [Node, ekka]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

stop_slave(Node) ->
    slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

node_id(Name) ->
    list_to_atom(lists:concat([Name, "@", host()])).

run_on(Node, Fun) ->
    %% Sending closures over erlang distribution is wrong, but for
    %% test purposes it should be ok.
    rpc:call(Node, erlang, apply, [Fun, []]).
