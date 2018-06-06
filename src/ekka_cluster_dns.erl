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

-module(ekka_cluster_dns).

-behaviour(ekka_cluster_strategy).

-import(proplists, [get_value/2]).

%% Cluster strategy Callbacks
-export([discover/1, lock/1, unlock/1, register/1, unregister/1]).

discover(Options) ->
    Name = get_value(name, Options),
    App  = get_value(app, Options),
    {ok, [node_name(App, IP) || IP <- inet_res:lookup(Name, in, a)]}.

node_name(App, IP) ->
    list_to_atom(App ++ "@" ++ inet_parse:ntoa(IP)).

lock(_Options) ->
    ignore.

unlock(_Options) ->
    ignore.

register(_Options) ->
    ignore.

unregister(_Options) ->
    ignore.

