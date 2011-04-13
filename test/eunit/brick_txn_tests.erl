%%% Copyright (c) 2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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

-module(brick_txn_tests).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").


%%%----------------------------------------------------------------------
%%% TESTS
%%%----------------------------------------------------------------------

all_tests_test_() ->
    {setup,
     fun test_setup/0,
     fun test_teardown/1,
     [
      ?_test(test_simple_txn1())
      , ?_test(test_simple_txn2a())
      , ?_test(test_simple_txn2b())
      , ?_test(test_simple_txn3a())
      , ?_test(test_simple_txn3b())
     ]}.

test_setup() ->
    brick_eunit_utils:setup_and_bootstrap().

test_teardown(X) ->
    brick_eunit_utils:teardown(X),
    ok.

%% @doc set transaction with TS less than old timestamp ... this
%% should fail
test_simple_txn1() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,
    ValALen = byte_size(ValA),

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,
    _ValBLen = byte_size(ValB),

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    ok = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = 1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ts_error,TS1}, ok] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}
           , {KeyB, TS3, ValA, 0, [{val_len,ValALen}]} %% TODO: bug!
          ], false}} = brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS equal to old timestamp but different
%% value ... this should fail
test_simple_txn2a() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,
    ValALen = byte_size(ValA),

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,
    _ValBLen = byte_size(ValB),

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    ok = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ts_error,TS1}, ok] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}
           , {KeyB, TS3, ValA, 0, [{val_len,ValALen}]} %% TODO: bug!
          ], false}} = brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS equal to old timestamp but same
%% value ... this should succeed
test_simple_txn2b() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,
    ValALen = byte_size(ValA),

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,
    _ValBLen = byte_size(ValB),

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    ok = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [ok, ok] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}
           , {KeyB, TS3, ValA, 0, [{val_len,ValALen}]}
          ], false}} = brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS larger than old timestamp but different
%% value ... this should succeed
test_simple_txn3a() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,
    ValALen = byte_size(ValA),

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,
    ValBLen = byte_size(ValB),

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    ok = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [ok, ok] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS2, ValB, 0, [{val_len,ValBLen}]}
           , {KeyB, TS3, ValA, 0, [{val_len,ValALen}]}
          ], false}} = brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS larger than old timestamp but same
%% value ... this should succeed
test_simple_txn3b() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,
    ValALen = byte_size(ValA),

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,
    _ValBLen = byte_size(ValB),

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    ok = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA, 0, [{val_len,ValALen}]}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [ok, ok] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS2, ValA, 0, [{val_len,ValALen}]}
           , {KeyB, TS3, ValA, 0, [{val_len,ValALen}]}
          ], false}} = brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.
