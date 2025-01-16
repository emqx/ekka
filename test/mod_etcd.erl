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

-module(mod_etcd).

-include_lib("inets/include/httpd.hrl").

-export([do/1]).

do(_Req = #mod{method = "GET", request_uri = "/v2/keys/" ++ _Uri}) ->
    Nodes = application:get_env(ekka, test_etcd_nodes, ["ct@127.0.0.1"]),
    Body = #{ <<"node">> =>
                  #{ <<"nodes">> =>
                         lists:map(fun(N) ->
                                           #{<<"key">> => list_to_binary("cl/" ++ N)}
                                   end,
                                   Nodes)
                   }
            },
    Response = {200, binary_to_list(jsone:encode(Body))},
    {proceed, [{response, Response}]};
do(_Req = #mod{request_uri = "/v2/keys/" ++ _Uri}) ->
    {proceed, [{response, {200, "{\"errorCode\": 0}"}}]};

do(Req) -> {proceed, Req#mod.data}.
