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
      ?_test(test_simple_txnset1())
      , ?_test(test_simple_txnset2a())
      , ?_test(test_simple_txnset2b())
      , ?_test(test_simple_txnset3a())
      , ?_test(test_simple_txnset3b())
      , ?_test(test_simple_txnset1r())
      , ?_test(test_simple_txnset2ar())
      , ?_test(test_simple_txnset2br())
      , ?_test(test_simple_txnset3ar())
      , ?_test(test_simple_txnset3br())
     ]}.

test_setup() ->
    brick_eunit_utils:setup_and_bootstrap().

test_teardown(X) ->
    brick_eunit_utils:teardown(X),
    ok.

%% @doc set transaction with TS less than old timestamp ... this
%% should fail
test_simple_txnset1() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = 1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    {txn_fail, [{1,{ts_error,TS1}}]} =
        brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS equal to old timestamp but different
%% value ... this should fail
test_simple_txnset2a() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    {txn_fail, [{1,{ts_error,TS1}}]} =
        brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS equal to old timestamp but same
%% value ... this should succeed
test_simple_txnset2b() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS2}, {ok,TS3}] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS larger than old timestamp but different
%% value ... this should succeed
test_simple_txnset3a() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS2}, {ok,TS3}] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS2, ValB}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS larger than old timestamp but same
%% value ... this should succeed
test_simple_txnset3b() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS2}, {ok,TS3}] = brick_simple:do(tab1, [txn, SetA, SetB]),

    %% get_many
    {ok, {[{KeyA, TS2, ValA}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.


%% @doc reverse order set transaction with TS less than old timestamp
%% ... this should fail
test_simple_txnset1r() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = 1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    {txn_fail, [{2,{ts_error,TS1}}]} =
        brick_simple:do(tab1, [txn, SetB, SetA]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc reverse order set transaction with TS equal to old timestamp
%% but different value ... this should fail
test_simple_txnset2ar() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    {txn_fail, [{2,{ts_error,TS2}}]} =
        brick_simple:do(tab1, [txn, SetB, SetA]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc set transaction with TS equal to old timestamp but same
%% value ... this should succeed
test_simple_txnset2br() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS3}, {ok,TS2}] = brick_simple:do(tab1, [txn, SetB, SetA]),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc reverse order set transaction with TS larger than old
%% timestamp but different value ... this should succeed
test_simple_txnset3ar() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,
    ValB = <<"BBB">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValB, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS3}, {ok,TS2}] = brick_simple:do(tab1, [txn, SetB, SetA]),

    %% get_many
    {ok, {[{KeyA, TS2, ValB}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.

%% @doc reverse order set transaction with TS larger than old
%% timestamp but same value ... this should succeed
test_simple_txnset3br() ->
    KeyPrefix = <<"/100/1">>,

    KeyA = <<"/100/1/A">>,
    ValA = <<"AAA">>,

    KeyB = <<"/100/1/B">>,

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, ValA),

    %% get_many
    {ok, {[{KeyA, TS1, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    %% txn set KeyA and set KeyB
    TS2 = TS1+1,
    SetA = brick_server:make_set(KeyA, TS2, ValA, 0, []),
    TS3 = 2,
    SetB = brick_server:make_set(KeyB, TS3, ValA, 0, []),
    [{ok,TS3}, {ok,TS2}] = brick_simple:do(tab1, [txn, SetB, SetA]),

    %% get_many
    {ok, {[{KeyA, TS2, ValA}, {KeyB, TS3, ValA}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100),

    ok.
