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

-module(ekka_cluster_etcd).

-behaviour(ekka_cluster_strategy).

-export([nodelist/1, register/1, unregister/1]).

nodelist(Options) ->
    AddrList = proplists:get_value(addr, Options),
    Prefix = proplists:get_value(prefix, Options),
    Path = with_prefix(Prefix, "/nodes"),
    case etcd_get(AddrList, Path, [{recursive, true}]) of
        {ok, Response} ->
            extract_nodes(Response);
        {error, Error} ->
            lager:error("Ekka(Etcd): nodelist error - ~p", [Error]),
            []
    end.

extract_nodes([]) -> [];
extract_nodes(Json) ->
  case maps:get(<<"nodes">>, maps:get(<<"node">>, Json), undefined) of
    undefined -> [];
    Values    -> [extract_node(V) || V <- Values]
  end.

with_prefix(Prefix, Path) ->
    Prefix ++ "/" ++ atom_to_list(ekka:cluster()) ++ Path.

extract_node(V) ->
    list_to_atom(binary_to_list(lists:last(binary:split(maps:get(<<"key">>, V), <<"/">>, [global])))).

register(Options) ->
    AddrList = proplists:get_value(addr, Options),
    Ttl = proplists:get_value(ttl, Options),
    Prefix = proplists:get_value(prefix, Options),
    Path = with_prefix(Prefix, "/nodes" ++ atom_to_list(node())),
    case etcd_set(AddrList, Path, [{ttl, Ttl}]) of
        {ok, _Response} -> ok;
        {error, Reason} -> {error, Reason}
    end.

unregister(_Etcd) ->
    ok.

etcd_get(AddrList, Key, Params) ->
    ekka_httpc:get(rand_addr(AddrList), Key, Params).

etcd_set(AddrList, Key, Params) ->
    ekka_httpc:put(rand_addr(AddrList), Key, Params).

rand_addr([Addr]) ->
    Addr;
rand_addr(AddrList) ->
    lists:nth(rand:uniform(lists:length(AddrList)), AddrList).

