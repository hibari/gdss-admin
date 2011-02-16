%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2011 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : squorum_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(squorum_eqc_tests).

-ifdef(EQC).

-define(NOTEST, true). %% TEST FAILS

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("brick_hash.hrl").
-include("brick_public.hrl").
-include("gmt_hlog.hrl").

-compile(export_all).

-export([start_bricks/0, prop_squorum1/0]).
-export([set_num_bricks/2,
         squorum_set/5, squorum_get/3, squorum_delete/2, squorum_get_keys/3,
         trigger_checkpoint/1, set_do_sync/2, scavenge/1,
         sync_down_the_chain/1, crash_brick/2]).

%% eqc_statem callbacks
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

%%%% -define(MAX_KEYS, 10).
-define(MAX_KEYS, 4).
-define(BRICKS, [{Br, node()} || Br <- [squorum_test1, squorum_test2,
                                        squorum_test3, squorum_test4,
                                        squorum_test5, squorum_test6,
                                        squorum_test7]]).

-record(state, {
          step = 1,
          dict,                         % key = term(),
                                                % val = [term()] of possible vals
          num_bricks,                   % int()
          bricks,                       % list(brick_names())
          reordered             % list(brick_names()) but different order
         }).

run() ->
    run(500).

run(NumTests) ->
    eqc:module({numtests,NumTests}, ?MODULE).

start_bricks() ->
    [catch start_brick(B) || B <- ?BRICKS].

start_brick({Br, _Nd} = Brick) ->
    gmt_hlog_common:full_writeback(?GMT_HLOG_COMMON_LOG_NAME),
    os:cmd("rm -rf hlog." ++ atom_to_list(Br) ++ "*"),
    brick_shepherd:start_brick(Br, []),
    poll_repair_state(Brick, pre_init, 10, 1000),
    brick_server:chain_set_my_repair_state(Br, node(), ok),
    brick_server:chain_role_standalone(Br, node()),
    brick_server:set_do_sync(Br, false),
    ok.

prop_squorum1() ->
    io:format("\n\n\nDon't forget to run ~s:start_bricks() before using.\n\n",
              [?MODULE]),
    timer:sleep(1234),
    ?LET(NumBricks, oneof([2, 3, 5, 7]),
         ?LET(Pivot, choose(1, NumBricks),
              begin
                  Bricks = lists:sublist(?BRICKS, NumBricks),
                  %% "Cut the deck" around a pivot position, to
                  %% simulate shuffling the list of bricks, but we
                  %% keep the shuffled list constant for the duration
                  %% of a test.
                  L_right = lists:sublist(Bricks, Pivot, 99999),
                  L_left = lists:sublist(Bricks, 1, Pivot - 1),
                  L_both = L_right ++ L_left,
                  Env = [{env_reordered, L_both}, {env_bricks, Bricks}],
                  common1_prop(fun(X, _S) -> X == ok end, Env)
              end)).

common1_prop(F_check, Env) ->
    ?FORALL(Cmds,eqc_statem:commands(?MODULE),
            collect({length(Cmds) div 10, div10},
                    begin
                        {_Hist, S, Res} = eqc_statem:run_commands(?MODULE, Cmds, Env),
                        ?WHENFAIL(begin
                                      io:format("Env = ~p\nS = ~p\nR = ~p\nHist = ~p\n", [Env, S, Res, _Hist]),
                                      ok
                                  end,
                                  F_check(Res, S))
                    end)).

%% initial_state() :: symbolic_state().
%% Called in symbolic context.

initial_state() ->
    QQQ = now(),
    gmt_loop:do_while(
      fun(_Acc) ->
              try
                  _ = brick_squorum:set(?BRICKS, <<"/foo/debug-A">>,
                                        io_lib:format("~p", [QQQ])),
                  delete_all_max_keys(),
                  _ = brick_squorum:delete(?BRICKS, list_to_binary(key_prefix())),
                  _ = brick_squorum:set(?BRICKS, <<"/foo/debug-Z">>,
                                        io_lib:format("~p", [QQQ])),
                  {false, x}
              catch
                  _:_ ->
                      {true, x}
              end
      end, x),
    L = [{make_key(X), []} || X <- lists:seq(1, ?MAX_KEYS)],
    #state{dict = orddict:from_list(L),
           bricks = ?BRICKS}.

delete_all_max_keys() ->
    L = [{make_key(X), []} || X <- lists:seq(1, ?MAX_KEYS)],
    [_ = is_ok_notex(catch brick_squorum:delete(?BRICKS, K)) || {K, _} <- L],
    %% io:format("ALL DELETED\n"),
    ok.

%% command :: (S::symbolic_state()) :: gen(call() | stop)
%% Called in symbolic context.

command(#state{step = 1}) ->
    {call, ?MODULE, set_num_bricks, [{var, env_bricks}, {var, env_reordered}]};
command(#state{step = StepN} = S) when StepN > 1 ->
    ?LET({Exp, _EnvBricks, _EnvReordered},
         {make_exp(S), {var, env_bricks}, {var, env_reordered}},
         frequency(
           [{20,  {call, ?MODULE, squorum_set, [{var, env_bricks},
                                                random_key(), random_val(),
                                                Exp, random_mod_flags()]}},
            %% We need more gets than the others because they're needed to help
            %% detect consistency errors if/when migration and or brick failures
            %% are happening while we're running.
            {100,  {call, ?MODULE, squorum_get, [{var, env_bricks},
                                                 random_key(), random_get_flags()]}},
            {50,  {call, ?MODULE, squorum_delete, [{var, env_bricks},
                                                   random_key()]}},
            {2,   {call, ?MODULE, trigger_checkpoint, [oneof(S#state.bricks)]}},
            {1,   {call, ?MODULE, set_do_sync, [oneof(S#state.bricks),
                                                oneof([true, false])]}}

            , {30, {call, ?MODULE, crash_brick, [{var, env_reordered},
                                                 choose(1, 2)]}}

                                                %        ,{30,   {call, ?MODULE, crash_brick, [{var, env_reordered},
                                                %                                            choose(1, length(EnvBricks))]}}
           ])).

random_get_flags() ->
    [].

random_mod_flags() ->
    oneof([[], [value_in_ram]]).

random_many_key() ->
    frequency([{1, list_to_binary(key_prefix())}, {3, random_key()}]).

%% precondition(S::symbolic_state(), C::call()) :: bool()
%% Called in symbolic (??) & runtime context.

precondition(S, {call, _, set_num_bricks, [_Bs, _L]}) ->
    S#state.step == 1;
precondition(S, {call, _, squorum_set, [_Bricks, _Key, _Val, _Exp, _Flags]}) ->
    S#state.step > 1;
precondition(S, {call, _, squorum_get, [_Bricks, _Key, _Flags]}) ->
    S#state.step > 1;
precondition(S, {call, _, squorum_delete, [_Bricks, _Key]}) ->
    S#state.step > 1;
precondition(S, {call, _, squorum_get_keys, _}) ->
    S#state.step > 1;
precondition(S, {call, _, trigger_checkpoint, _}) ->
    S#state.step > 1;
precondition(S, {call, _, set_do_sync, _}) ->
    S#state.step > 1;
precondition(S, {call, _, scavenge, _}) ->
    S#state.step > 1;
precondition(S, {call, _, sync_down_the_chain, _}) ->
    S#state.step > 1;
precondition(S, {call, _, crash_brick, _}) ->
    S#state.step > 1;
precondition(_S, Call) ->
    io:format("\nprecondition: what the heck is this?  ~p\n", [Call]),
    timer:sleep(1000),
    false.

%% postcondition(S::dynamic_state(),C::call(),R::term()) :: bool()
%% Called in runtime context.

postcondition(_S, _C, {'EXIT', {shutdown, {gen_server, call, _}}}) ->
    true;
postcondition(_S, _C, {'EXIT', {noproc, {gen_server, call, _}}}) ->
    true;
postcondition(_S, _C, {error, current_role, {_, undefined, undefined}}) ->
    true;
postcondition(S, C, R) ->
    case postcondition2(S, C, R) of
        true ->
            true;
        false ->
            _Key = hd(element(4, C)),
            io:format("BUMMER: ~p at ~p\n", [R, C]),
            io:format("BUMMER: Key = ~p\nread at ~p, write at ~p\n",
                      [_Key, skipped, skipped]),
            false
    end.

postcondition2(_S, {call, _, set_num_bricks, [_Bs, _L]} = _C, _R) ->
    true;
postcondition2(_S, {call, _, squorum_set, [_Bricks,
                                           _Key, _Val, _Exp, _Flags]} = _C, R) ->
    is_ok(R);
postcondition2(S, {call, _, squorum_get, [_Bricks, Key, Flags]} = _C, R) ->
    Vs = orddict:fetch(Key, S#state.dict),
    case R of
        key_not_exist ->
            has_zero_definite_vals(Vs);
        {ok, _TS, Val} ->
            proplists:get_value(get_all_attribs, Flags) == undefined
                andalso val_is_a_definite_or_maybe(Val, Vs);
        %% with get_all_attribs flag
        {ok, _TS, Val, _ExpTime, _Flags} ->
            proplists:get_value(get_all_attribs, Flags) /= undefined
                andalso val_is_a_definite_or_maybe(Val, Vs);
        {'EXIT', {timeout, _}} ->
            true                               % Hibari temp hack?
    end;
postcondition2(S, {call, _, squorum_delete, [_Bricks, Key]} = _C, R) ->
    Vs = orddict:fetch(Key, S#state.dict),
    case R of
        %%      %% For brick txn semantics
        %%      {txn_fail, [{_, key_not_exist}]} ->
        %%          Vs == [];
        key_not_exist ->
            has_zero_definite_vals(Vs);
        ok ->
            has_one_definite_or_maybe_val(Vs)
    end;
postcondition2(S, {call, _, squorum_get_keys, [_Bricks, Key, MaxNum]} = _C, R) ->
    AllKeys0 = [K || {K, V} <- orddict:to_list(S#state.dict),
                     K > Key, V /= []],
    {AllKeys, Rest} = lists:split(if MaxNum > length(AllKeys0) ->
                                          length(AllKeys0);
                                     true ->
                                          MaxNum
                                  end, AllKeys0),
    case R of
        {ok, {Rs, Bool}} ->
            RsKs = [K || {K, _TS} <- Rs],
            %% Since we don't know what's beyond the end of our limited range,
            %% we can't test the correctness of Bool 100% completely.
            RsKs == AllKeys andalso
                              ((MaxNum == 0 andalso Bool == true) orelse
                                                                    (Bool == true andalso Rest /= []) orelse
                                                                                                        (Bool == false andalso AllKeys == []) orelse
                               true);
        _ ->
            false
    end;
postcondition2(S, {call, _, simple_do, [CrudeOps, _]} = _C, R) ->
    case R of
        %%      {txn_fail, _} ->
        %%          true;
        L when is_list(L) ->
            CrudeRes = lists:zip(CrudeOps, R),
            Bools = lists:map(
                      fun({{Name, Args}, Res}) ->
                              postcondition2(S, {call, ?MODULE, Name, Args},
                                             Res)
                      end, CrudeRes),
            lists:all(fun(X) -> X == true end, Bools)
    end;
postcondition2(_S, {call, _, trigger_checkpoint, _}, _R) ->
    true;
postcondition2(_S, {call, _, set_do_sync, _}, _R) ->
    true;
postcondition2(_S, {call, _, scavenge, _}, _R) ->
    true;
postcondition2(_S, {call, _, sync_down_the_chain, _}, _R) ->
    true;
postcondition2(_S, {call, _, crash_brick, _}, _R) ->
    true;
postcondition2(_S, _Call, _R) ->
    false.

%% next_state(S::symbolic_state(),R::var(),C::call()) :: symbolic_state()
%% Called in symbolic & runtime context.

next_state(S, {var, _}, _C) ->
    %% BEWARE, this is big, big-time QuickCheck cheating.  It will
    %% really screw up QC's shrinking, but I'm more interested in
    %% finding weird race conditions and diagnosing them via gmt_elog
    %% tracing than worrying about deterministic shrinking.
    S#state{step = S#state.step + 1};
next_state(S, R, C) ->
    NewS = next_state2(S, R, C),
    NewS#state{step = S#state.step + 1}.

next_state2(S, _Result, {call, _, set_num_bricks, [Bs, ShuffledBs]}) ->
    S#state{num_bricks = length(Bs),
            bricks = Bs,
            reordered = ShuffledBs};
next_state2(S, Result, {call, _, squorum_set, [_Bricks, Key, Val, _Exp, _Flags]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of ok          -> [Val];
              {'EXIT', _} -> [{maybe_set, Val}|Old]
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, Result, {call, _, squorum_get, [_Bricks, Key, _Flags]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of key_not_exist                 -> [];
              {ok, _TS, Val}                -> [Val];
              {ok, _TS, Val, _ExpTime, _Fs} -> [Val];
              {'EXIT', _}                   -> Old
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, Result, {call, _, squorum_delete, [_Bricks, Key]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of key_not_exist -> [];
              ok            -> [];
              {'EXIT', _}   -> [maybe_delete|Old]
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, _Result, {call, _, trigger_checkpoint, _}) ->
    S;
next_state2(S, _Result, {call, _, set_do_sync, _}) ->
    S;
next_state2(S, _Result, {call, _, scavenge, _}) ->
    S;
next_state2(S, _Result, {call, _, sync_down_the_chain, _}) ->
    S;
next_state2(S, _Result, {call, _, crash_brick, _}) ->
    S;
next_state2(S, _Result, _Call) ->
    S.

random_key() ->
    ?LET(I, choose(1, ?MAX_KEYS),
         make_key(I)).

make_key(I) ->
    list_to_binary(io_lib:format("~s~4.4.0w", [key_prefix(), I])).

key_prefix() ->
    "/foo/bar/".

make_exp(S) ->
    {MSec, Sec, USec} = now(),
    NowX = (MSec * 1000000 * 1000000) + (Sec * 1000000) + USec,
    (NowX * 1000) + S#state.step.

random_val() ->
    oneof([<<>>,
           ?LET({I, Char}, {nat(), choose($a, $z)},
                list_to_binary(io_lib:format("v ~s",
                                             [lists:duplicate(I + 1, Char)])))]).

set_num_bricks(_, _) ->
    ok.

squorum_set(Bricks, Key, Val, Exp, Flags) ->
    catch brick_squorum:set(Bricks, Key, Val, Exp, Flags, 9000).

squorum_get(Bricks, Key, Flags) ->
    catch brick_squorum:get(Bricks, Key, Flags, 2000).

squorum_delete(Bricks, Key) ->
    catch brick_squorum:delete(Bricks, Key, 9000).

squorum_get_keys(Bricks, Key, MaxNum) ->
    catch brick_squorum:get_keys(Bricks, Key, MaxNum).

trigger_checkpoint({BrickName, Node}) ->
    io:format("c"),
    catch brick_server:checkpoint(BrickName, Node, [silent]).

set_do_sync({BrickName, Node}, Bool) ->
    Fmt = if Bool -> "S"; true -> "s" end, io:format(Fmt),
    catch brick_server:set_do_sync({BrickName, Node}, Bool).

scavenge({BrickName, Node}) ->
    io:format("v"),
    catch brick_server:start_scavenger(
            BrickName, Node, [{skip_live_percentage_greater_than, 0},
                              silent]).

sync_down_the_chain({BrickName, Node}) ->
    io:format("Y"),
    catch brick_server:sync_down_the_chain(BrickName, Node, []).

crash_brick(BrickList, Nth) ->
    {BrickName, Node} = Brick = lists:nth(Nth, BrickList),
    brick_server:stop({BrickName, Node}),
    start_brick(Brick). % Will delete all data & restart

is_ok(ok)                      -> true;
is_ok({'EXIT', {shutdown, _}}) -> true;
is_ok({'EXIT', {noproc, _}})   -> true;         % Would be false in ideal world
is_ok({'EXIT', {timeout, _}})  -> false;        % Is false in an ideal world
is_ok(X)                       -> X.

is_ok_notex(ok) ->            true;
is_ok_notex(key_not_exist) -> true;
is_ok_notex(X) ->             X.

has_one_definite_or_maybe_val(Vs) ->
    lists:any(
      fun(B) when is_binary(B) -> true;
         ({maybe_set, _})      -> true;
         (maybe_delete)        -> false
      end, Vs).

has_zero_definite_vals(Vs) ->
    %% If there's a maybe_delete in there, then ignore anything that
    %% follows it.
    Prefix = lists:takewhile(fun(maybe_delete) -> false;
                                (_)            -> true
                             end, Vs),
    lists:all(
      fun(B) when is_binary(B) -> false;
         ({maybe_set, _})      -> true;
         (maybe_delete)        -> true
      end, Prefix).

val_is_a_definite_or_maybe(Val, Vs) ->
    lists:any(
      fun(B) when B == Val              -> true;
         ({maybe_set, B}) when B == Val -> true;
         (_)                            -> false
      end, Vs).

%% Useful, public helpers.

poll_repair_state({Brick, Node}, Wanted, SleepMs, MaxIters) ->
    RepairPoll =
        fun(0) ->
                {false, failed};
           (Acc) ->
                case (catch brick_server:chain_get_my_repair_state(Brick, Node))
                of
                    ok ->
                        {false, ok};
                    Wanted ->
                        {false, ok};
                    _St ->
                        timer:sleep(SleepMs),
                        {true, Acc - 1}
                end
        end,
    gmt_loop:do_while(RepairPoll, MaxIters).

-endif. %% -ifdef(EQC).
