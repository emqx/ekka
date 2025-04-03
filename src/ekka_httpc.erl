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

-module(ekka_httpc).

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

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

get(Addr, Path, Params) ->
    get(Addr, Path, Params, []).

get(Addr, Path, Params, Headers) ->
    get(Addr, Path, Params, Headers, []).

get(Addr, Path, Params, Headers, HttpOpts) ->
    URL = build_url(Addr, Path, Params),
    parse_response(hackney:request(get, URL, Headers, <<>>, [with_body | HttpOpts])).

post(Addr, Path, Params) ->
    post(Addr, Path, Params, []).

post(Addr, Path, Params, HttpOpts) ->
    URL = build_url(Addr, Path),
    Body = build_query(Params),
    Headers = [{<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}],
    parse_response(hackney:request(post, URL, Headers, Body, [with_body | HttpOpts])).

put(Addr, Path, Params) ->
    put(Addr, Path, Params, []).

put(Addr, Path, Params, HttpOpts) ->
    URL = build_url(Addr, Path),
    Body = build_query(Params),
    Headers = [{<<"Content-Type">>, <<"application/x-www-form-urlencoded">>}],
    parse_response(hackney:request(put, URL, Headers, Body, [with_body | HttpOpts])).

delete(Addr, Path, Params) ->
    delete(Addr, Path, Params, []).

delete(Addr, Path, Params, HttpOpts) ->
    URL = build_url(Addr, Path, Params),
    parse_response(hackney:request(delete, URL, [], <<>>, HttpOpts)).

-spec(build_url(string(), string()) -> binary()).
build_url(Addr, Path) ->
    iolist_to_binary([Addr, "/", Path]).

build_url(Addr, Path, Params) ->
    iolist_to_binary([build_url(Addr, Path), "?", build_query(Params)]).

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

parse_response({ok, Status, RespHeaders, Body}) when Status =:= 200;
                                                     Status =:= 201 ->
    case hackney_headers:parse(<<"content-type">>, RespHeaders) of
        {<<"application">>, <<"json">>, _} ->
            {ok, jsone:decode(Body)};
        CC ->
            {error, {unexpected_content_type, CC}}
    end;
parse_response({ok, 204, _RespHeaders, _}) ->
    {ok, []};
parse_response({ok, Status, _RespHeaders, Body}) ->
    {error, {Status, Body}};
parse_response({error, Reason}) ->
    {error, Reason}.
