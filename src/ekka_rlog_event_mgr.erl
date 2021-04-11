%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(ekka_rlog_event_mgr).

-behaviour(gen_event).

%% API:
-export([start_link/0, subscribe_events/0, unsubscribe_events/1, notify_shard_up/1,
         wait_for_shards/2]).

%% gen_event callbacks:
-export([init/1, handle_call/2, handle_event/2]).

-define(SERVER, ?MODULE).

-record(s,
        { ref        :: reference()
        , subscriber :: pid()
        }).

-include("ekka_rlog.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_event:start_link({local, ?SERVER}, []).

-spec notify_shard_up(ekka_rlog:shard()) -> ok.
notify_shard_up(Shard) ->
    gen_event:notify(?SERVER, {shard_up, Shard}).

-spec subscribe_events() -> reference().
subscribe_events() ->
    Self = self(),
    Ref  = make_ref(),
    ok = gen_event:add_sup_handler(?SERVER, {?MODULE, Ref}, [Ref, Self]),
    Ref.

-spec unsubscribe_events(reference()) -> ok.
unsubscribe_events(Ref) ->
    ok = gen_event:delete_handler(?SERVER, {?MODULE, Ref}, []),
    flush_events(Ref).

-spec wait_for_shards([ekka_rlog:shard()], timeout()) -> ok | {timeout, [ekka_rlog:shard()]}.
wait_for_shards(Shards, Timeout) ->
    ?tp(notice, "Waiting for shards",
        #{ shards => Shards
         , timeout => Timeout
         }),
    TRef = ekka_rlog_lib:send_after(Timeout, self(), timeout),
    ERef = subscribe_events(),
    %% Exclude shards that are up, since they are not going to send any events:
    DownShards = Shards -- ets:match(?replica_tab, {{?upstream_node, $1}, $_}),
    Ret = do_wait_shards(ERef, DownShards),
    ekka_rlog_lib:cancel_timer(TRef),
    unsubscribe_events(ERef),
    ?tp(notice, "Done waiting for shards",
        #{ shards => Shards
         , result =>  Ret
         }),
    Ret.

%%================================================================================
%% gen_event callbacks
%%================================================================================

init([Ref, Subscriber]) ->
    State = #s{ ref = Ref
              , subscriber = Subscriber
              },
    {ok, State, hibernate}.

handle_call(_, State) ->
    {ok, {error, unknown_call}, State, hibernate}.

handle_event(Event, State = #s{ref = Ref, subscriber = Sub}) ->
    Sub ! {Ref, Event},
    {ok, State, hibernate}.

%%================================================================================
%% Internal functions
%%================================================================================

do_wait_shards(_, []) ->
    ok;
do_wait_shards(ERef, RemainingShards) ->
    receive
        {ERef2, Event} when ERef =:= ERef2 ->
            case Event of
                {shard_up, Shard} ->
                    do_wait_shards(ERef, RemainingShards -- [Shard]);
                _ ->
                    do_wait_shards(ERef, RemainingShards)
            end;
        timeout ->
            {timeout, RemainingShards}
    end.

flush_events(ERef) ->
    receive
        {ERef, _} ->
            flush_events(ERef)
    after 0 ->
            ok
    end.
