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

-module(ekka_cluster_k8s).

-behaviour(ekka_cluster_strategy).

-import(proplists, [get_value/2, get_value/3]).

%% Cluster strategy callbacks.
-export([discover/1, lock/1, unlock/1, register/1, unregister/1]).

-define(SERVICE_ACCOUNT_PATH, "/var/run/secrets/kubernetes.io/serviceaccount/").

-define(LOG(Level, Format, Args), lager:Level("Ekka(k8s): " ++ Format, Args)).

%%--------------------------------------------------------------------
%% ekka_cluster_strategy callbacks
%%--------------------------------------------------------------------

discover(Options) ->
    Server = get_value(apiserver, Options),
    Service = get_value(service_name, Options),
    App = get_value(app_name, Options, "ekka"),
    AddrType = get_value(address_type, Options, ip),
    Namespace = get_value(namespace, Options, "default"),
    case k8s_service_get(Server, Service, Namespace) of
        {ok, Response} ->
            Addresses = extract_addresses(AddrType, Response, Namespace),
            {ok, [node_name(App, Addr) || Addr <- Addresses]};
        {error, Reason} ->
            {error, Reason}
    end.

node_name(App, Addr) ->
    list_to_atom(App ++ "@" ++ binary_to_list(Addr)).

lock(_Options) ->
    ignore.

unlock(_Options) ->
    ignore.

register(_Options) ->
    ignore.

unregister(_Options) ->
    ignore.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

k8s_service_get(Server, Service, Namespace) ->
    Headers = [{"Authorization", "Bearer " ++ token()}],
    HttpOpts = case filelib:is_file(cert_path()) of
                   true  -> [{ssl, [{cacertfile, cert_path()}]}];
                   false -> [{ssl, [{verify, verify_none}]}]
               end,
    ekka_httpc:get(Server, service_path(Service, Namespace), [], Headers, HttpOpts).

service_path(Service, Namespace) ->
    lists:concat(["api/v1/namespaces/", Namespace, "/endpoints/", Service]).

% namespace() ->
%     binary_to_list(trim(read_file("namespace", <<"default">>))).

token() ->
    binary_to_list(trim(read_file("token", <<"">>))).

cert_path() -> ?SERVICE_ACCOUNT_PATH ++ "/ca.crt".

read_file(Name, Default) ->
    case file:read_file(?SERVICE_ACCOUNT_PATH ++ Name) of
        {ok, Data}     -> Data;
        {error, Error} -> ?LOG(error, "Cannot read ~s: ~p", [Name, Error]),
                          Default
    end.

trim(S) -> binary:replace(S, <<"\n">>, <<>>).

extract_addresses(Type, Response, Namespace) ->
    lists:flatten(
      [[ extract_host(Type, Addr, Namespace)
         || Addr <- maps:get(<<"addresses">>, Subset, [])]
            || Subset <- maps:get(<<"subsets">>, Response, [])]).

extract_host(ip, Addr, _) ->
    maps:get(<<"ip">>, Addr);

extract_host(dns, Addr, Namespace) ->
    Ip = binary:replace(maps:get(<<"ip">>, Addr), <<".">>, <<"-">>),
    iolist_to_binary([Ip, ".", Namespace, ".pod.cluster.local"]).

