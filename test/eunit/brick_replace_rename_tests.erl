%%% Copyright (c) 2015 Hibari developers.  All rights reserved.
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

-module(brick_replace_rename_tests).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 15000).

-type exp_time() :: integer().

%%%----------------------------------------------------------------------
%%% TESTS
%%%----------------------------------------------------------------------

all_tests_test_() ->
    {setup,
     fun test_setup/0,
     fun test_teardown/1,
     [
      ?_test(test_simple_replace1()),
      ?_test(test_simple_rename1())
     ]}.

test_setup() ->
    brick_eunit_utils:setup_and_bootstrap().

test_teardown(X) ->
    brick_eunit_utils:teardown(X),
    ok.

%% @doc test exp_time_directive and attrib_directive in replace operation
test_simple_replace1() ->
    KeyPrefix = <<"/100/1">>,
    Key    = <<"/100/1/A">>,

    ValA   = <<"AAA">>,
    ExpA   = make_exp(5000 * 1000),
    FlagsA = [{color, blue}],

    ValB   = <<"BBB">>,
    ExpB   = make_exp(360 * 1000),
    FlagsB = [{shape, triangle}],

    ValC   = <<"CCC">>,
    FlagsC = [{color, green}, {exp_time_directive, keep}, {attrib_directive, keep}],

    %% reset
    _ = brick_simple:delete(tab1, Key),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, Key, ValA, ExpA, FlagsA, ?TIMEOUT),

    %% get_many
    {ok, {[{Key, _TS1, ValA, ExpA, Flags1}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    blue = proplists:get_value(color, Flags1),

    %% replace KeyA
    {ok, _} = brick_simple:replace(tab1, Key, ValB, ExpB, FlagsB, ?TIMEOUT),
    {ok, {[{Key, _TS2, ValB, ExpB, Flags2}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    undefined = proplists:get_value(color, Flags2),
    triangle = proplists:get_value(shape, Flags2),

    %% replace KeyA again
    {ok, _} = brick_simple:replace(tab1, Key, ValC, FlagsC),
    {ok, {[{Key, _TS3, ValC, ExpB, Flags3}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    green = proplists:get_value(color, Flags3),
    triangle = proplists:get_value(shape, Flags3).

%% @doc test exp_time_directive and attrib_directive in rename operation
test_simple_rename1() ->
    KeyPrefix = <<"/100/1">>,

    KeyA   = <<"/100/1/A">>,
    Val    = <<"AAA">>,
    ExpA   = make_exp(5000 * 1000),
    FlagsA = [{color, blue}],

    KeyB   = <<"/100/1/B">>,
    ExpB   = make_exp(360 * 1000),
    FlagsB = [{shape, triangle}, {exp_time_directive, replace}, {attrib_directive, replace}],

    KeyC   = <<"/100/1/C">>,
    FlagsC = [{color, green}],

    %% reset
    _ = brick_simple:delete(tab1, KeyA),
    _ = brick_simple:delete(tab1, KeyB),
    _ = brick_simple:delete(tab1, KeyC),

    %% add KeyA
    {ok, _} = brick_simple:add(tab1, KeyA, Val, ExpA, FlagsA, ?TIMEOUT),

    %% get_many
    {ok, {[{KeyA, _TS1, Val, ExpA, Flags1}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    blue = proplists:get_value(color, Flags1),

    %% rename KeyA to KeyB
    {ok, _} = brick_simple:rename(tab1, KeyA, KeyB, ExpB, FlagsB, ?TIMEOUT),
    {ok, {[{KeyB, _TS2, Val, ExpB, Flags2}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    undefined = proplists:get_value(color, Flags2),
    triangle = proplists:get_value(shape, Flags2),

    %% rename KeyB to KeyC
    {ok, _} = brick_simple:rename(tab1, KeyB, KeyC, FlagsC),
    {ok, {[{KeyC, _TS3, Val, ExpB, Flags3}], false}} =
        brick_simple:get_many(tab1, KeyPrefix, 100, [get_all_attribs]),
    green = proplists:get_value(color, Flags3),
    triangle = proplists:get_value(shape, Flags3).

-spec make_exp(non_neg_integer()) -> exp_time().
make_exp(StepMillis) ->
    NowX = gmt_time_otp18:erlang_system_time(micro_seconds),
    %% TODO: FIXME: This should read (NowX div 1000) or
    %% just NowX with erlang_system_time(milli_seconds).
    %% https://github.com/hibari/gdss-admin/issues/11
    (NowX * 1000) + StepMillis.
