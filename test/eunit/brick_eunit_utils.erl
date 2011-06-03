%%% Copyright (c) 2009-2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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

-module(brick_eunit_utils).

-export([setup/0, setup/1, setup_and_bootstrap/0, setup_and_bootstrap/1]).
-export([teardown/0, teardown/1]).

-define(APPS, [gdss_admin, gdss_client, gdss_brick, gmt_util, inets, crypto, sasl]).

-define(MYNODE, 'gdss_eunit@localhost').


%%%----------------------------------------------------------------------
%%% TEST UTILS
%%%----------------------------------------------------------------------

setup() ->
    setup(false).

setup(Verbose) ->
    teardown(unused),
    os:cmd("rm -rf Schema.local hlog.* root"),
    os:cmd("ln -s ../../gdss_admin/priv/root ."),
    os:cmd("epmd -kill; sleep 1"),
    os:cmd("epmd -daemon; sleep 1"),
    case net_kernel:start([?MYNODE, shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}, _}} ->
            %% TODO: doesn't match documentation
            ok
    end,
    [ application:load(A) || A <- ?APPS ],
    ok = application:set_env(sasl, errlog_type, error),
    ok = application:set_env(gdss_brick, brick_max_log_size_mb, 1),
    ok = application:set_env(gdss_brick, brick_min_log_size_mb, 1),
    [ application:start(A) || A <- lists:reverse(?APPS) ],
    if Verbose ->
            noop;
       true ->
            error_logger:delete_report_handler(error_logger_tty_h)
    end,
    random:seed(erlang:now()),
    ok.

setup_and_bootstrap() ->
    setup_and_bootstrap(false).

setup_and_bootstrap(Verbose) ->
    setup(Verbose),
    brick_admin:bootstrap_local([], true, $/, 3, 1, 1, []),
    wait_for_tables(),
    ok.

teardown() ->
    teardown(unused).

teardown(_) ->
    catch exit(whereis(brick_admin_sup), kill),
    [ application:stop(A) || A <- ?APPS ],
    case node() of
        ?MYNODE ->
            net_kernel:stop();
        _ ->
            ok
    end,
    ok.

wait_for_tables() ->
    wait_for_tables(node()).

wait_for_tables(GDSSAdmin) ->
    gmt_loop:do_while(fun poll_table/1, {GDSSAdmin,not_ready,tab1}).

poll_table({GDSSAdmin,not_ready,Tab}) ->
    TabCh = gmt_util:atom_ify(gmt_util:list_ify(Tab) ++ "_ch1"),
    case rpc:call(GDSSAdmin, brick_sb, get_status, [chain, TabCh]) of
        {ok, healthy} ->
            {false, ok};
        _ ->
            ok = timer:sleep(50),
            {true, {GDSSAdmin,not_ready,Tab}}
    end.
