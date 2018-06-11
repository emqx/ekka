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

-module(ekka_test).

-compile(export_all).
-compile(nowarn_export_all).

start_slave(node, Node) ->
    {ok, N} = slave:start(host(), Node, ebin_path()),
    N;
start_slave(ekka, Node) ->
    {ok, Ekka} = slave:start(host(), Node, ebin_path()),
    rpc:call(Ekka, application, ensure_all_started, [ekka]),
    Ekka.

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

host() -> [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    EbinDir = local_path(["ebin"]),
    DepsDir = local_path(["deps", "*", "ebin"]),
    "-pa " ++ EbinDir ++ " -pa " ++ DepsDir.

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

