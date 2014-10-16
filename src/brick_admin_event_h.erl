%%%-------------------------------------------------------------------
%%% Copyright (c) 2011-2014 Hibari developers.  All rights reserved.
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
%%%
%%% File    : brick_admin_event_h.erl
%%% Purpose : Brick administrator alarm event handler
%%%-------------------------------------------------------------------

-module(brick_admin_event_h).

-include("gmt_elog.hrl").

-behaviour(gen_event).

-define(SERVER, ?MODULE).

-include("partition_detector.hrl").

%% API
-export([start_link/1, add_handler/1]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).


-record(state, {}).

%%====================================================================
%% gen_event callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link(_) -> {ok,Pid} | {error,Error}
%% Description: Creates an event manager.
%%--------------------------------------------------------------------
start_link(StartArg) ->
    gen_event:start_link(StartArg).

%%--------------------------------------------------------------------
%% Function: add_handler(_) -> ok | {'EXIT',Reason} | term()
%% Description: Adds an event handler
%%--------------------------------------------------------------------
add_handler(HandlerSpec) ->
    gen_event:add_sup_handler(HandlerSpec, ?MODULE, []).

%%====================================================================
%% gen_event callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}
%% Description: Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%--------------------------------------------------------------------
init(_Args) ->
    ?ELOG_INFO("init: ~p", [_Args]),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function:
%% handle_event(Event, State) -> {ok, State} |
%%                               {swap_handler, Args1, State1, Mod2, Args2} |
%%                               remove_handler
%% Description:Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is called for
%% each installed event handler to handle the event.
%%--------------------------------------------------------------------
handle_event({set_alarm, {{alarm_network_heartbeat, {Node, AorB}}, _Descr}}, State) ->
    %% TODO: This event should probably also be included in the scoreboard,
    %%       but there is no category for non-brick, non-chain events in
    %%       the scoreboard right now.
    if AorB == 'A' ->
            ?ELOG_INFO("node ~p heartbeats on 'A' network in "
                       "alarm, disconnecting from network "
                       "distribution", [Node]),
            net_kernel:disconnect(Node);
       AorB == 'B' ->
            ?ELOG_INFO("node ~p heartbeats on 'B' network are "
                       "in alarm.", [Node])
    end,
    {ok, State};
handle_event({beacon_event, FromAddr, FromPort, B}, State)
  when is_record(B, beacon) ->
    case proplists:get_value(brick_admin, B#beacon.extra) of
        undefined ->
            ok;
        {Phase, _StartTime, Node, _Pid} ->
            if Node == node() ->
                    ok;
               Phase == starting ->
                    ?ELOG_INFO("Duplicate Admin Server trying to start: ~p ~p ~p",
                               [FromAddr, FromPort, B]),
                    ok;
               true ->
                    ?ELOG_WARNING("Duplicate Admin Server: ~p ~p ~p",
                                  [FromAddr, FromPort, B]),
                    timer:sleep(1),             % log to flushes to disk?
                    _ = spawn(fun() -> _ = application:stop(gdss_brick),
                                       timer:sleep(200),
                                       erlang:halt() end),
                    ok
            end;
        _Other ->
            ?ELOG_ERROR("bad beacon: ~p ~p ~p",
                        [FromAddr, FromPort, B]),
            ok
    end,
    {ok, State};
handle_event(_Event, State) ->
    ?ELOG_INFO("handle_event: ~p", [_Event]),
    {ok, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_call(Request, State) -> {ok, Reply, State} |
%%                                {swap_handler, Reply, Args1, State1,
%%                                  Mod2, Args2} |
%%                                {remove_handler, Reply}
%% Description: Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified event
%% handler to handle the request.
%%--------------------------------------------------------------------
handle_call(_Request, State) ->
    ?ELOG_INFO("handle_call: ~p", [_Request]),
    {ok, unknown_call, State}.

%%--------------------------------------------------------------------
%% Function:
%% handle_info(Info, State) -> {ok, State} |
%%                             {swap_handler, Args1, State1, Mod2, Args2} |
%%                              remove_handler
%% Description: This function is called for each installed event handler when
%% an event manager receives any other message than an event or a synchronous
%% request (or a system message).
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    ?ELOG_INFO("handle_info: ~p", [_Info]),
    {ok, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description:Whenever an event handler is deleted from an event manager,
%% this function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Function: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
