%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_httpc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> ekka_ct:all(?MODULE).

%% We use erlang's httpd as the target server
init_per_suite(Config) ->
    inets:start(),
    application:ensure_all_started(hackney),
    {ok, Pid} = inets:start(httpd, [ {port, 0}
                                   , {server_name, "ekka_test"}
                                   , {modules, [mod_esi]}
                                   , {erl_script_alias, {"/test", [?MODULE]}}
                                   , {server_root, "."}
                                   , {document_root, "."}
                                   ]),
    [{port, Port}] = httpd:info(Pid, [port]),
    Server = "http://localhost:" ++ integer_to_list(Port),
    [{server, Server}, {httpd, Pid} | Config].

end_per_suite(Config) ->
    inets:stop(httpd, proplists:get_value(httpd, Config)),
    Config.

%% mod_esi callbacks:
echo(SessionId, Env, Input) ->
    Data = make_data(?FUNCTION_NAME, Env, Input),
    mod_esi:deliver(SessionId, "Status: 200 OK\r\nContent-type: application/json\r\n\r\n"),
    mod_esi:deliver(SessionId, Data).

'201'(SessionId, Env, Input) ->
    Data = make_data(?FUNCTION_NAME, Env, Input),
    mod_esi:deliver(SessionId, "Status: 201 Created\r\nContent-type: application/json\r\n\r\n"),
    mod_esi:deliver(SessionId, Data).

'204'(SessionId, Env, Input) ->
    io:format(user, "204~n  Env=~p~n  Input=~p~n", [Env, Input]),
    mod_esi:deliver(SessionId, "Status: 204 No Content\r\n\r\n").

%% Testcases:
t_get(Config) ->
    Server = proplists:get_value(server, Config),
    %% 200:
    ?assertEqual(
       {ok, #{<<"foo">> => <<"bar">>, <<"x">> => <<"y">>}},
       ekka_httpc:get(Server,
                      "test/ekka_httpc_SUITE:echo",
                      [{<<"foo">>, <<"bar">>}, {<<"x">>, <<"y">>}])),
    %% 204:
    ?assertEqual(
       {ok, []},
       ekka_httpc:get(Server, "test/ekka_httpc_SUITE:204", [])),
    %% Not found:
    ?assertMatch({error, {404, _}}, ekka_httpc:get(Server, "test/ekka_httpc_SUITE:not_implemented", [])),
    %% Not implemented:
    ?assertMatch({error, {501, _}}, ekka_httpc:get(Server, "something", [])).

%% Run request towards an IPv6 listener that does not exist. Check
%% that IP address was parsed correctly.
t_get_ipv6(_) ->
    ?assertMatch(
       {error, econnrefused},
       ekka_httpc:get("http://[::]:12833", "", [])).

t_post(Config) ->
    Server = proplists:get_value(server, Config),
    %% 200:
    ?assertEqual(
       {ok, #{<<"foo">> => <<"bar">>, <<"x">> => <<"y">>}},
       ekka_httpc:post(Server,
                       "test/ekka_httpc_SUITE:echo",
                       [{<<"foo">>, <<"bar">>}, {<<"x">>, <<"y">>}])),
    %% 201:
    ?assertEqual(
       {ok, #{<<"foo">> => <<"bar">>, <<"x">> => <<"y">>}},
       ekka_httpc:post(Server,
                       "test/ekka_httpc_SUITE:201",
                       [{<<"foo">>, <<"bar">>}, {<<"x">>, <<"y">>}])),
    %% Not found:
    ?assertMatch({error, {404, _}}, ekka_httpc:post(Server, "test/ekka_httpc_SUITE:not_implemented", [])),
    %% Not implemented:
    ?assertMatch({error, {501, _}}, ekka_httpc:post(Server, "something", [])).

t_put(Config) ->
    Server = proplists:get_value(server, Config),
    %% 204:
    ?assertEqual(
       {ok, []},
       ekka_httpc:put(Server,
                      "test/ekka_httpc_SUITE:204",
                      [{<<"foo">>, <<"bar">>}, {<<"x">>, <<"y">>}])).

t_delete(Config) ->
    Server = proplists:get_value(server, Config),
    ?assertMatch(
       {ok, _},
       ekka_httpc:delete(Server, "test/ekka_httpc_SUITE:204", [])).

t_build_url(_) ->
   ?assertEqual(<<"localhost/nodes/1">>,
                ekka_httpc:build_url("localhost", "nodes/1")),
   ?assertEqual(<<"localhost/nodes/1?a=b&c=d">>,
                ekka_httpc:build_url("localhost", "nodes/1", [{a, b}, {c, d}])),
   ?assertEqual(<<"localhost/nodes/1?a=b%2Fc%2Fd">>,
                ekka_httpc:build_url("localhost", "nodes/1", [{a, "b/c/d"}])),
   ?assertEqual(<<"localhost/nodes/1?a=b%2Fc%2Fd&e=5">>,
                ekka_httpc:build_url("localhost", "nodes/1", [{a, "b/c/d"}, {e, 5}])).

%% Misc.

make_data(Name, Env, Input) ->
    logger:info("~p~n  Env=~p~n  Input=~p~n", [Name, Env, Input]),
    try
        QL = [{iolist_to_binary(K), iolist_to_binary(V)} || {K, V} <- uri_string:dissect_query(Input)],
        jsone:encode(QL)
    catch
        EC:Err:Stack ->
            logger:error("~p~n  ~p:~p:~p", [Name, EC, Err, Stack]),
            []
    end.
