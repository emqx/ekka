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

-module(ekka_autoclean).

-include("ekka.hrl").

-export([init/0, check/1]).

-define(LOG(Level, Format, Args),
        logger:Level("Ekka(Autoclean): " ++ Format, Args)).

-record(autoclean, {expiry, timer}).

-type(autoclean() :: #autoclean{}).

-export_type([autoclean/0]).

init() ->
    case ekka:env(cluster_autoclean) of
        {ok, Expiry} -> timer_backoff(#autoclean{expiry = Expiry});
        undefined    -> undefined
    end.

timer_backoff(State = #autoclean{expiry = Expiry}) ->
    TRef = ekka_node_monitor:run_after(Expiry div 4, autoclean),
    State#autoclean{timer = TRef}.

check(State = #autoclean{expiry = Expiry}) ->
    [maybe_clean(Member, Expiry) || Member <- ekka_membership:members(down)],
    timer_backoff(State).

maybe_clean(Member = #member{node = Node, ltime = LTime}, Expiry) ->
    case expired(LTime, Expiry) of
        true ->
            ?LOG(warning, "Node ~s has been down for ~p seconds, attempting to remove it "
                          "from the cluster.",
                 [Node, Expiry div 1000]),
            force_leave(Member, Expiry);
        false -> ok
    end.

force_leave(Member = #member{node = Node}, Expiry) ->
    case ekka_cluster:force_leave(Node, fun() -> still_down(Member) end) of
        ok ->
            ?LOG(warning, "Removed node ~s from the cluster after ~p seconds down.",
                 [Node, Expiry div 1000]);
        ignore ->
            ?LOG(info, "Membership record of node ~s changed before its removal; keeping it.",
                 [Node]);
        {error, Reason} ->
            ?LOG(warning, "Could not remove node ~s from the cluster: ~p. The member is "
                          "kept; the next check retries.", [Node, Reason]);
        Other ->
            ?LOG(warning, "Removal of node ~s returned ~p; the next check retries.",
                 [Node, Other])
    end.

%% The member record is replaced when it is refreshed, and a running node must not be
%% removed by this stale check.
still_down(#member{node = Node} = Member) ->
    Member =:= ekka_membership:lookup_member(Node) andalso
        not lists:member(Node, ekka_mnesia:running_nodes()).

expired(LTime, Expiry) ->
    timer:now_diff(erlang:timestamp(), LTime) div 1000 > Expiry.

