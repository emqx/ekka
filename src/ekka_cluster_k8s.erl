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

-module(ekka_cluster_k8s).

-behaviour(ekka_cluster_strategy).

%% Cluster strategy Callbacks
-export([nodelist/1, register/1, unregister/1]).

nodelist(Options) ->
    Addr = proplists:get_value(addr, Options),
    Path = proplists:get_value(path, Options),
    AppName = proplists:get_value(app_name, Options, "ekka"),
    case ekka_httpc:get(Addr, Path, []) of
        {ok, Response} ->
            Nodes = [list_to_atom(AppName ++ "@" ++ Ip) || Ip <- extract_addresses(Response)],
            [Node || Node <- Nodes, ekka_node:is_aliving(Node)];
        {error, Error} ->
            lager:error("Ekka(k8s): nodelist error - ~p", [Error]), []
    end.

extract_addresses(Json) ->
  [Subsets|_] = maps:get(<<"subsets">>, Json),
  case maps:get(<<"addresses">>, Subsets, undefined) of
    undefined -> [];
    Addresses -> [extract_ip(A) || A <- Addresses]
  end.

extract_ip(A) -> binary_to_list(maps:get(<<"ip">>, A)).

register(_Options) ->
    ignore.

unregister(_Options) ->
    ok.

