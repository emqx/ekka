%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_cluster_strategy).

-export([discover/2, lock/2, unlock/2, register/2, unregister/2]).

-export_type([options/0]).

-type(options() :: proplists:proplist()).

-callback(discover(options()) -> {ok, [node()]} | {error, term()}).

-callback(lock(options()) -> ok | ignore | {error, term()}).

-callback(unlock(options()) -> ok | ignore | {error, term()}).

-callback(register(options()) -> ok | ignore | {error, term()}).

-callback(unregister(options()) -> ok | ignore | {error, term()}).

-spec discover(module(), options()) -> {ok, list(node())} | {error, term()}.
discover(Mod, Options) ->
    Mod:discover(Options).

-spec lock(module(), options()) -> ok | ignore | {error, term()}.
lock(Mod, Options) ->
    Mod:lock(Options).

-spec unlock(module(), options()) -> ok | ignore | {error, term()}.
unlock(Mod, Options) ->
    Mod:unlock(Options).

-spec register(module(), options()) -> ok | ignore | {error, term()}.
register(Mod, Options) ->
    Mod:register(Options).

-spec unregister(module(), options()) -> ok | ignore | {error, term()}.
unregister(Mod, Options) ->
    Mod:unregister(Options).
