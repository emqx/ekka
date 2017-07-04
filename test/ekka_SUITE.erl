%%--------------------------------------------------------------------
%% Copyright (c) 2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(ekka_SUITE).

-compile(export_all).

-include("ekka.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").


all() ->
     [{group, cluster}].

groups() ->
    [{cluster, [sequence],
     [cluster_test,
      cluster_join,
      cluster_leave,
      cluster_remove,
      cluster_remove2,
      cluster_node_down
     ]}].

init_per_suite(Config) ->
    NewConfig = generate_config(),
    Vals = proplists:get_value(ekka, NewConfig),
    [application:set_env(ekka, Par, Value) || {Par, Value} <- Vals],
    application:ensure_all_started(ekka),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ekka_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% cluster group
%%--------------------------------------------------------------------
cluster_test(_Config) ->
    ok.

cluster_join(_Config) ->
    ok.

cluster_leave(_Config) ->
    ok.

cluster_remove(_Config) ->
    ok.

cluster_remove2(_Config) ->
    ok.

cluster_node_down(_Config) ->
    ok.

host() -> [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

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

slave(ekka, Node) ->
    ensure_slave(),
    {ok, Ekka} = slave:start(host(), Node),
    rpc:call(Ekka, application, ensure_all_started, [ekka]),
    Ekka;

slave(node, Node) ->
    ensure_slave(),
    {ok, N} = slave:start(host(), Node, "-pa ../../ebin -pa ../../deps/*/ebin"),
    N.

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "ekka.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "ekka.conf"])]),
    cuttlefish_generator:map(Schema, Conf).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

ensure_slave() ->
    EbinDir = local_path(["ebin"]),
    DepsDir = local_path(["deps", "*", "ebin"]),
    code:add_paths(lists:appead([[EbinDir, DepsDir]])),
    code:clash(),
    ok.
