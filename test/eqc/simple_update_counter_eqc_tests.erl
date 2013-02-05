%%%-------------------------------------------------------------------
%%% Copyright (c) 2011-2013 Hibari developers.  All rights reserved.
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
%%% File    : simple_update_counter_eqc_tests.erl
%%% Purpose : Simple test to illustrate a client-based counter
%%%           implementation.
%%%-------------------------------------------------------------------

-module(simple_update_counter_eqc_tests).

-export([update_counter/4]).

%%====================================================================
%% Types - common
%%====================================================================

-type key() :: brick_simple_stub:key().
-type table() :: brick_simple_stub:table().
-type ts() :: brick_simple_stub:ts().

%%====================================================================
%% Types - external
%%====================================================================

-type update_counter_reply() :: {ok, integer()} | invalid_arg_present | {non_integer,ts()} | no_return.

%%====================================================================
%% Types - internal
%%====================================================================


%%====================================================================
%% API
%%====================================================================

-spec update_counter(table(), key(), integer(), timeout()) -> update_counter_reply().

%% doc
%% - This function updates a counter with a positive or negative
%%   integer. However, counters can never become less than zero.
%% - If Incr is not an integer, return `'invalid_arg_present'`.
%% - If two (or more) callers perform update_counter/3 simultaneously,
%%   both updates will take effect without the risk of losing one of the
%%   updates. The new value `{'ok', NewVal}` of the counter is returned.
%% - If Key doesn't exist, a new counter is created with the value Incr
%%   if it is larger than 0, otherwise it is set to 0.
%% - If Key exists but it's value is not an integer greater than or equal
%%   to zero, return {non_integer, timestamp()}.
%% - If updating of the counter exceeds the specified timeout, exit by
%%   Timeout.

update_counter(Tab, Key, Incr, Timeout) when is_integer(Incr) ->
    update_counter2(Tab, Key, Incr, Timeout, erlang:now());
update_counter(_Tab, _Key, _Incr, _Timeout) ->
    invalid_arg_present.


%%====================================================================
%% Internal
%%====================================================================

update_counter1(Tab, Key, Incr, Timeout, Expires) ->
    case timer:now_diff(erlang:now(), Expires) of
        TDiff when TDiff > Timeout*1000 ->
            exit(timeout);
        _ ->
            update_counter2(Tab, Key, Incr, Timeout, Expires)
    end.

update_counter2(Tab, Key, Incr, Timeout, Expires) ->
    case brick_simple:get(Tab, Key, [], Timeout) of
        {ok, TS, Val} ->
            IntVal =
                try
                    binary_to_integer(Val)
                catch
                    exit:badarg ->
                        undefined
                end,
            if is_integer(IntVal) ->
                    NewIntVal = update_counter_value(IntVal, Incr),
                    NewVal = integer_to_binary(NewIntVal),
                    case brick_simple:replace(Tab, Key, NewVal, 0, [{testset, TS}], Timeout) of
                        ok ->
                            {ok, NewIntVal};
                        {ts_error, _} ->
                            %% retry
                            update_counter1(Tab, Key, Incr, Timeout, Expires);
                        Err ->
                            Err
                    end;
               true ->
                    {non_integer, TS}
            end;
        key_not_exists ->
            NewIntVal = update_counter_value(0, Incr),
            NewVal = integer_to_binary(NewIntVal),
            case brick_simple:add(Tab, Key, NewVal, 0, [], Timeout) of
                {ok, _} ->
                    {ok, NewIntVal};
                key_exists ->
                    %% retry
                    update_counter1(Tab, Key, Incr, Timeout, Expires);
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

update_counter_value(Val, Incr) ->
    case Val + Incr of
        X when X > 0 ->
            X;
        _ ->
            0
    end.

binary_to_integer(X) ->
    erlang:list_to_integer(erlang:binary_to_list(X)).

integer_to_binary(X) ->
    erlang:list_to_binary(erlang:integer_to_list(X)).
