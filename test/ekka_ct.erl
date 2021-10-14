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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% @doc Get all the test cases in a CT suite.
all(Suite) ->
    lists:usort([F || {F, 1} <- Suite:module_info(exports),
                      string:substr(atom_to_list(F), 1, 2) == "t_"
                ]).

cleanup(Testcase) ->
    ct:pal("Cleaning up after ~p...", [Testcase]),
    application:stop(ekka),
    mria:stop(),
    mnesia:stop(),
    ok = mnesia:delete_schema([node()]).

start_slave(NodeOrEkka, #{name := Name, env := Env}) ->
    start_slave(NodeOrEkka, Name, Env);
start_slave(NodeOrEkka, Name) when is_atom(Name) ->
    start_slave(NodeOrEkka, Name, []).

start_ekka(#{node := Node, join_to := JoinTo}) ->
    ok = rpc:call(Node, ekka, start, []),
    case rpc:call(Node, ekka, join, [JoinTo]) of
        ok -> ok;
        ignore -> ok
    end,
    Node.

start_slave(node, Name, Env) ->
    CommonBeamOpts = "+S 1:1 " % We want VMs to only occupy a single core
        "-kernel inet_dist_listen_min 3000 "
        "-kernel inet_dist_listen_max 3050 ",
    {ok, Node} = slave:start_link(host(), Name, CommonBeamOpts ++ ebin_path()),
    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during ekka start:
    {ok, _} = cover:start([Node]),
    CommonEnv = [{gen_rpc, port_discovery, stateless}],
    setenv(Node, Env ++ CommonEnv),
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
    ok = cover:stop([Node]),
    rpc:call(Node, mnesia, stop, []),
    mnesia:delete_schema([Node]),
    slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

vals_to_csv(L) ->
    string:join([lists:flatten(io_lib:format("~p", [N])) || N <- L], ",") ++ "\n".

setenv(Node, Env) ->
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].
