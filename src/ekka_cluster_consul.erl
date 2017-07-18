%%%===================================================================
%%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(ekka_cluster_consul).

-export([bootstrap/1, register/1, unregister/1]).

-record(consul, {addr, svc}).

bootstrap(Options) ->
    Addr = proplists:get_value(addr, Options),
    Svc = proplists:get_value(consul_svc, Options),
    Consul = #consul{addr = Addr, svc = Svc},
    case nodelist(Consul) of
        {ok, Nodes} ->
            {ok, Consul, Nodes};
        {error, Reason} ->
            {error, Reason}
    end.

nodelist(#consul{addr = Addr, svc = Svc}) ->
    Path = "v1/health/service/" ++ Svc,
    case ekka_httpc:get(Addr, Path, [{tag, ekka:cluster()}]) of
        {ok, Json} ->
            {ok, extract_nodes(Json)};
        {error, Reason} ->
            {error, Reason}
    end.

register(_Consul) ->
    ok.

unregister(_Consul) ->
    ok.

extract_nodes(_Json) ->
    [].

