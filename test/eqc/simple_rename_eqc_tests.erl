%%%-------------------------------------------------------------------
%%% Copyright (c) 2011-2017 Hibari developers.  All rights reserved.
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
%%% File    : simple_rename_eqc_tests.erl
%%% Purpose : Simple test to illustrate a client-based rename
%%%           implementation.
%%%-------------------------------------------------------------------

-module(simple_rename_eqc_tests).

-export([rename/6]).

%%====================================================================
%% Types - common
%%====================================================================

-type attr() :: brick_simple_stub:attr().
-type key() :: brick_simple_stub:key().
-type table() :: brick_simple_stub:table().
-type time_t() :: brick_simple_stub:time_t().
-type ts() :: brick_simple_stub:ts().

%%====================================================================
%% Types - external
%%====================================================================

-type rename_flag() :: {testset, ts()}
                     | value_in_ram
                     | attr().

-type rename_reply() :: {ok, ts()} | key_not_exist | {ts_error, ts()}.

%%====================================================================
%% Types - internal
%%====================================================================


%%====================================================================
%% API
%%====================================================================

-spec rename(table(), key(), key(), time_t(), [rename_flag()], timeout()) -> rename_reply().

%% @doc
%% - This function renames an existing value corresponding to Key to
%%   NewKey and deletes the Key.
%% - If Key doesn't exist or Key and NewKey are same, return
%%   `'key_not_exist'`.
%% - If Flags contains `{'testset', timestamp()}`, and there is a
%%   timestamp mismatch with the Key, return `{'ts_error',
%%   timestamp()}`.

rename(_Tab, Key, Key, _ExpTime, _Flags, _Timeout) ->
    key_not_exist;
rename(Tab, Key, NewKey, ExpTime, Flags, Timeout) ->
    GetFlags =
        case proplists:lookup(testset, Flags) of
            none ->
                [];
            Flag ->
                [Flag]
        end,
    case brick_simple:get(Tab, Key, GetFlags, Timeout) of
        {ok, _TS, Val} ->
            SetFlags = proplists:delete(testset, Flags),
            Res = brick_simple:set(Tab, NewKey, Val, ExpTime, SetFlags, Timeout),
            brick_simple:delete(Tab, Key, Timeout),
            Res;
        Err ->
            Err
    end.
