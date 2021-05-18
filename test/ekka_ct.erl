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

-type env() :: [{atom(), atom(), term()}].

-type start_spec() ::
        #{ name    := atom()
         , node    := node()
         , join_to => node()
         , env     := env()
         , number  := integer()
         }.

-type node_spec() :: ekka_rlog:role() % name automatically, use default environment
                   | {ekka_rlog:role(), env()} % name automatically
                   | {ekka_rlog:role(), atom()} % give name, use default environment
                   | {ekka_rlog:role(), atom(), env()}. % customize everything

%% @doc Generate cluster config with all necessary connectivity
%% options, that should be able to run on the localhost
-spec cluster([node_spec()], env()) -> [start_spec()].
cluster(Specs0, CommonEnv) ->
    Specs1 = lists:zip(Specs0, lists:seq(1, length(Specs0))),
    Specs = expand_node_specs(Specs1, CommonEnv),
    CoreNodes = [node_id(Name) || {{core, Name, _}, _} <- Specs],
    %% Assign grpc ports:
    BaseGenRpcPort = 9000,
    GenRpcPorts = maps:from_list([{node_id(Name), {tcp, BaseGenRpcPort + Num}}
                                  || {{_, Name, _}, Num} <- Specs]),
    %% Set the default node of the cluster:
    JoinTo = case CoreNodes of
                 [First|_] -> #{join_to => First};
                 _         -> #{}
             end,
    [JoinTo#{ name   => Name
            , node   => node_id(Name)
            , env    => [ {ekka, core_nodes, CoreNodes}
                        , {ekka, node_role, Role}
                        , {gen_rpc, tcp_server_port, BaseGenRpcPort + Number}
                        , {gen_rpc, client_config_per_node, {internal, GenRpcPorts}}
                        | Env]
            , number => Number
            }
     || {{Role, Name, Env}, Number} <- Specs].

start_cluster(node, Specs) ->
    Nodes = [start_slave(node, I) || I <- Specs],
    mnesia:delete_schema(Nodes),
    Nodes;
start_cluster(ekka, Specs) ->
    start_cluster(node, Specs),
    [start_ekka(I) || I <- Specs];
start_cluster(ekka_async, Specs) ->
    Ret = start_cluster(node, Specs),
    spawn(fun() -> [start_ekka(I) || I <- Specs] end),
    Ret.

teardown_cluster(Specs) ->
    Nodes = [I || #{node := I} <- Specs],
    [rpc:call(I, mnesia, stop, []) || I <- Nodes],
    [ok = stop_slave(I) || I <- Nodes],
    ok.

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

write(Record) ->
    ?tp_span(trans_write, #{record => Record, txid => get_txid()},
             mnesia:write(Record)).

read(Tab, Key) ->
    ?tp_span(trans_read, #{tab => Tab, txid => get_txid()},
             mnesia:read(Tab, Key)).

start_slave(node, Name, Env) ->
    CommonBeamOpts = "+S 1:1 " % We want VMs to only occupy a single core
        "-kernel inet_dist_listen_min 3000 " % Avoid collisions with gen_rpc ports
        "-kernel inet_dist_listen_max 3050 ",
    {ok, Node} = slave:start_link(host(), Name, CommonBeamOpts ++ ebin_path()),
    %% Load apps before setting the enviroment variables to avoid
    %% overriding the environment during ekka start:
    [rpc:call(Node, application, load, [App]) || App <- [gen_rpc]],
    ok = mnesia:delete_schema([Node]),
    {ok, _} = cover:start([Node]),
    %% Disable gen_rpc listener by default:
    Env1 = [{gen_rpc, tcp_server_port, false}|Env],
    setenv(Node, Env1),
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
    mnesia:delete_schema([Node]),
    ok = cover:stop([Node]),
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

set_network_delay(N) ->
    ok = file:write_file("/tmp/nemesis", integer_to_list(N) ++ "us\n").

vals_to_csv(L) ->
    string:join([lists:flatten(io_lib:format("~p", [N])) || N <- L], ",") ++ "\n".

setenv(Node, Env) ->
    [rpc:call(Node, application, set_env, [App, Key, Val]) || {App, Key, Val} <- Env].

expand_node_specs(Specs, CommonEnv) ->
    lists:map(
      fun({Spec, Num}) ->
              {case Spec of
                   core ->
                       {core, gen_node_name(Num), CommonEnv};
                   replicant ->
                       {replicant, gen_node_name(Num), CommonEnv};
                   {Role, Name} when is_atom(Name) ->
                       {Role, Name, CommonEnv};
                   {Role, Env} when is_list(Env) ->
                       {Role, gen_node_name(Num), CommonEnv ++ Env};
                   {Role, Name, Env} ->
                       {Role, Name, CommonEnv ++ Env}
               end, Num}
      end,
      Specs).

gen_node_name(N) ->
    list_to_atom("n" ++ integer_to_list(N)).

get_txid() ->
    case mnesia:get_activity_id() of
        {_, TID, _} ->
            TID
    end.
