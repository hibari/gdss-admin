%%% Copyright (c) 2009-2013 Hibari developers.  All rights reserved.
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

-module(brick_basic_tests).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%%----------------------------------------------------------------------
%%% TESTS
%%%----------------------------------------------------------------------

setup() ->
    %% true to enable sasl output
    X = brick_eunit_utils:setup_and_bootstrap(true),
    X.

teardown(X) ->
    brick_eunit_utils:teardown(X),
    ok.

setup_noop() ->
    %% true to enable sasl output
    error_logger:delete_report_handler(error_logger_tty_h),
    ok.

teardown_noop(_X) ->
    ok.

eunit_brick_squorum_t0_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_squorum:t0()) end)]}
    }.

eunit_chain_t35_test_() ->
    {setup, fun setup/0, fun teardown/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:chain_t35([])) end)]}
    }.

eunit_single_brick_regression_test_() ->
    {setup, fun setup/0, fun teardown/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:single_brick_regression()) end)]}
    }.

eunit_chain_all_test_() ->
    {setup, fun setup/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:chain_all()) end)]}
    }.

eunit_brick_itimer_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_itimer_test:start_test_manually()) end)]}
    }.

%% TBD - porting
%% eunit_reliablefifo_brick0_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, test_reliablefifo:brick0()) end)]}
%%     }.

%% TBD - porting
%% eunit_reliablefifo_chain_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, test_reliablefifo:cl_chain_reliablefifo()) end)]}
%%     }.

%% TBD - porting
%% eunit_scav_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, test_scav:test_all()) end)]}
%%     }.

%% TBD - porting
%% eunit_scav_get_busy_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, test_scav:get_busy()) end)]}
%%     }.

%% TBD - porting
%% eunit_scav2_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, test_scav2:test_all()) end)]}
%%     }.

-ifdef(GMTQC).

eunit_eqc_hlog_local_eqc__t1_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_hlog_local_eqc__t1(500)) end)]}
    }.

eunit_eqc__hlog_eqc__t1_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_hlog_eqc__t1(500)) end)]}
    }.

eunit_eqc_hlog_blackbox_eqc__t1_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_hlog_blackbox_eqc__t1(500)) end)]}
    }.

eunit_eqc_my_pread_eqc__t1_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_my_pread_eqc__t1(500)) end)]}
    }.

%% TBD - failing
%% eunit_eqc_repair_eqc__t1_test_() ->
%%     {setup, fun setup_noop/0, fun teardown_noop/1,
%%      {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_repair_eqc__t1(500)) end)]}
%%     }.

eunit_eqc_repair_eqc__t2_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_repair_eqc__t2(500)) end)]}
    }.

eunit_eqc_simple_eqc__t1_test_() ->
    {setup, fun setup_noop/0, fun teardown_noop/1,
     {timeout, 600, [?_test(fun() -> ?assertEqual(ok, brick_test0:cl_eqc_simple_eqc__t1(500)) end)]}
    }.

%% NOTE: This QC model can fail with false positives (deleting more
%% copies than quorum size).  Do not use until fixed.
%% ?_test(fun() -> ?assertEqual(ok, cl_eqc_hlog_eqc__t2([100])) end),

%% NOTE: This QC model can fail with false positives (scribble over
%% the length byte of the last hunk_summ header in a file will not
%% cause a failure).  Do not use until fixed.
%% ?_test(fun() -> ?assertEqual(ok, cl_eqc_hlog_eqc__t2([100])) end),

-endif. %% GMTQC

