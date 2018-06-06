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

-module(ekka_node).

%% Node API
-export([is_aliving/1, is_running/1, is_running/2, parse_name/1]).

%% @doc Is node aliving?
-spec(is_aliving(node()) -> boolean()).
is_aliving(Node) ->
    lists:member(Node, nodes()) orelse net_adm:ping(Node) =:= pong.

-spec(is_running(atom()) -> boolean()).
is_running(App) ->
    lists:keymember(App, 1, application:which_applications()).

%% @doc Is the application running?
-spec(is_running(node(), atom()) -> boolean()).
is_running(Node, App) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc, _}  -> false;
        Applications -> lists:keymember(App, 1, Applications)
    end.

%% @doc Parse node name
-spec(parse_name(string()) -> atom()).
parse_name(Name) when is_list(Name) ->
    case string:tokens(Name, "@") of
        [_Node, _Host] -> list_to_atom(Name);
        _              -> with_host(Name)
    end.

with_host(Name) ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    list_to_atom(lists:concat([Name, "@", Host])).

