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

-module(ekka_httpc).

-export([start/0, stop/0]).

-export([ get/3
        , get/4
        , get/5
        , post/3
        , post/4
        , put/3
        , put/4
        , delete/3
        , delete/4
        ]).

-define(PROFILE, ?MODULE).
-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

start() ->
    case inets:start(httpc, [{profile, ?PROFILE}]) of
        {ok, _Pid} ->
            httpc:set_options(httpc_profile_opts(), ?PROFILE);
        {error, {already_started, _Pid}} ->
            httpc:set_options(httpc_profile_opts(), ?PROFILE);
        {error, Reason} ->
            {error, Reason}
    end.

stop() ->
    inets:stop(httpc, ?PROFILE).

get(Addr, Path, Params) ->
    get(Addr, Path, Params, []).

get(Addr, Path, Params, Headers) ->
    get(Addr, Path, Params, Headers, []).

get(Addr, Path, Params, Headers, HttpOpts) ->
    Req = {build_url(Addr, Path, Params), Headers},
    parse_response(httpc_request(get, Req, [{autoredirect, true} | HttpOpts])).

post(Addr, Path, Params) ->
    post(Addr, Path, Params, []).

post(Addr, Path, Params, HttpOpts) ->
    Req = {build_url(Addr, Path), [], "application/x-www-form-urlencoded", build_query(Params)},
    parse_response(httpc_request(post, Req, [{autoredirect, true} | HttpOpts])).

put(Addr, Path, Params) ->
    put(Addr, Path, Params, []).

put(Addr, Path, Params, HttpOpts) ->
    Req = {build_url(Addr, Path), [], "application/x-www-form-urlencoded", build_query(Params)},
    parse_response(httpc_request(put, Req, [{autoredirect, true} | HttpOpts])).

delete(Addr, Path, Params) ->
    delete(Addr, Path, Params, []).

delete(Addr, Path, Params, HttpOpts) ->
    Req = {build_url(Addr, Path, Params), []},
    parse_response(httpc_request(delete, Req, HttpOpts)).

httpc_request(Method, Req, HttpOpts) ->
    httpc:request(Method, Req, HttpOpts, [], ?PROFILE).

httpc_profile_opts() ->
    %% NOTE
    %% If host has at least one non-scope-local non-loopback IPv6 address,
    %% consider it IPv6-capable. This check skips resolver (because it can
    %% introduce non-trivial delay in the startup sequence), and thus is
    %% imprecise.
    case host_ipv6_addrs() of
        {ok, [_ | _]} ->
            [{ipfamily, inet6fb4}];
        _Otherwise ->
            []
    end.

%% @doc Return non-scope-local IPv6 addresses for a host, assigned to any
%% non-loopback network interface.
host_ipv6_addrs() ->
    case inet:getifaddrs() of
        {ok, IfAddrs} ->
            Addrs = lists:flatmap(
                fun({_IfName, Props}) ->
                    Flags = proplists:get_value(flags, Props, []),
                    Up = lists:member(up, Flags),
                    Running = lists:member(running, Flags),
                    Loopback = lists:member(loopback, Flags),
                    case Up andalso Running andalso not Loopback of
                        true ->
                            iface_ipv6_addrs(Props);
                        false ->
                            []
                    end
                end,
                IfAddrs
            ),
            {ok, Addrs};
        Error ->
            Error
    end.

%% @doc Return all non-scope-local IPv6 addresses for a network interface.
iface_ipv6_addrs(Props) ->
    [
        Addr
     || {addr, Addr = {O1, _, _, _, _, _, _, _}} <- Props,
        O1 =/= 16#FE80
    ].

-spec build_url(string(), string()) -> string().
build_url(Addr, Path) ->
    lists:concat([Addr, "/", Path]).

build_url(Addr, Path, Params) ->
    lists:concat([build_url(Addr, Path), "?", build_query(Params)]).

-if(?OTP_RELEASE >= 23).
build_query(Params) when is_list(Params) ->
    uri_string:compose_query([{safty(K), safty(V)} || {K, V} <- Params]).

safty(A) when is_atom(A)    -> atom_to_list(A);
safty(I) when is_integer(I) -> integer_to_list(I);
safty(T) -> T.

-else.
build_query(Params) ->
    string:join([urlencode(Param) || Param <- Params], "&").

urlencode(L) when is_list(L) ->
    http_uri:encode(L);
urlencode({K, V}) ->
    urlencode(K) ++ "=" ++ urlencode(V);
urlencode(A) when is_atom(A) ->
    urlencode(atom_to_list(A));
urlencode(I) when is_integer(I) ->
    urlencode(integer_to_list(I));
urlencode(B) when is_binary(B) ->
    urlencode(binary_to_list(B)).
-endif.

parse_response({ok, {{_, Code, _}, _Headers, Body}}) ->
    parse_response({ok, Code, Body});
parse_response({ok, {Code, Body}}) ->
    parse_response({ok, Code, Body});
parse_response({ok, 200, Body}) ->
    {ok, jsone:decode(iolist_to_binary(Body))};
parse_response({ok, 201, Body}) ->
    {ok, jsone:decode(iolist_to_binary(Body))};
parse_response({ok, 204, _Body}) ->
    {ok, []};
parse_response({ok, Code, Body}) ->
    {error, {Code, Body}};
parse_response({error, Reason}) ->
    {error, Reason}.
