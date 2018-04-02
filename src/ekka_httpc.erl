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

-module(ekka_httpc).

-export([get/3, get/4, get/5, post/3, put/3, delete/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

get(Addr, Path, Params) ->
    get(Addr, Path, Params, []).

get(Addr, Path, Params, Headers) ->
    get(Addr, Path, Params, Headers, []).

get(Addr, Path, Params, Headers, HttpOpts) ->
    Req = {build_url(Addr, Path, Params), Headers},
    parse_response(httpc:request(get, Req, [{autoredirect, true} | HttpOpts], [])).

post(Addr, Path, Params) ->
    Req = {build_url(Addr, Path), [], "application/x-www-form-urlencoded", urlencode(Params)},
    parse_response(httpc:request(post, Req, [], [])).

put(Addr, Path, Params) ->
    Req = {build_url(Addr, Path), [], "application/x-www-form-urlencoded", urlencode(Params)},
    parse_response(httpc:request(put, Req, [], [])).

delete(Addr, Path, Params) ->
    Req = {build_url(Addr, Path, Params), []},
    parse_response(httpc:request(delete, Req, [], [])).

build_url(Addr, Path) ->
    lists:concat([Addr, "/", Path]).

build_url(Addr, Path, Params) ->
    lists:concat([build_url(Addr, Path), "?", urlencode(Params)]).

urlencode(Params) ->
    string:join([percent_encode(Param) || Param <- Params], "&").

percent_encode(L) when is_list(L) ->
    http_uri:encode(L);
percent_encode({K, V}) ->
    percent_encode(K) ++ "=" ++ percent_encode(V);
percent_encode(A) when is_atom(A) ->
    percent_encode(atom_to_list(A));
percent_encode(I) when is_integer(I) ->
    percent_encode(integer_to_list(I));
percent_encode(B) when is_binary(B) ->
    percent_encode(binary_to_list(B)).

parse_response({ok, {{_, Code, _}, _Headers, Body}}) ->
    parse_response({ok, Code, Body});
parse_response({ok, 200, Body}) ->
    {ok, jsx:decode(iolist_to_binary(Body), [return_maps])};
parse_response({ok, 201, Body}) ->
    {ok, jsx:decode(iolist_to_binary(Body), [return_maps])};
parse_response({ok, 204, _Body}) ->
    {ok, []};
parse_response({ok, Code, Body}) ->
    {error, {Code, Body}};
parse_response({error, Reason}) ->
    {error, Reason}.

