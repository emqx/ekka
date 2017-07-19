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

-module(ekka_cluster_dns).

-behaviour(ekka_cluster_strategy).

%% Cluster strategy Callbacks
-export([nodelist/1, register/1, unregister/1]).

-type(option() :: {name, string()} | {app, atom()}).

-spec(nodelist(list(option())) -> list(node())).
nodelist(Options) ->
    Name = proplists:get_value(name, Options),
    App = proplists:get_value(app, Options),
    [node_name(App, IP) || IP <- inet_res:lookup(Name, in, a)].

node_name(App, IP) ->
    list_to_atom(App ++ "@" ++ inet_parse:ntoa(IP)).

register(_Options) ->
    ignore.
    
unregister(_Options) ->
    ok.
