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
%%% File    : simple_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(simple_eqc_tests).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-define(GMTQC, proper).
-undef(EQC).
-endif. %% -ifdef(PROPER).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(GMTQC, eqc).
-undef(PROPER).
-endif. %% -ifdef(EQC).

-ifdef(GMTQC).

-include("brick_hash.hrl").
-include("brick_public.hrl").

-compile(export_all).

%% eqc_statem callbacks
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

%%%% -define(MAX_KEYS, 10).
-define(MAX_KEYS, 4).
-define(TABLE, tab1).

-record(state, {
          step = 1,
          dict,                         %% key = term(),
          %% val = [term()] of possible vals
          bricks                        %% list(brick_names())
         }).

run() ->
    run(500).

run(NumTests) ->
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

prop_simple1() ->
    common1_prop(fun(X, S) -> X == ok andalso ets_table_sizes_match_p(S) end,
                 []).

%% Use the *_noproc_ok() versions when chain lengths are changing or
%% when data migrations are happening.

prop_simple1_noproc_ok() ->
    common1_prop(fun(X, S) ->
                         case X of
                             ok ->
                                 true andalso ets_table_sizes_match_p(S);
                             %% NOTE: following is tainted by smart_exceptions
                             {postcondition,
                              {'EXIT', {_, _, {error, current_role, _}}}} ->
                                 true;
                             %% NOTE: following is tainted by smart_exceptions
                             {postcondition,
                              {'EXIT', {_, _, {error, current_repair_state, _}}}} ->
                                 true;
                             _ ->
                                 false
                         end
                 end, []).

common1_prop(F_check, Env) ->
    io:format("\n\nHibari: get_many() has been commented out from command()\n\n"),
    timer:sleep(2000),
    ?FORALL(Cmds, commands(?MODULE),
            collect({length(Cmds) div 10, div10},
                    begin
                        {_Hist, S, Res} = run_commands(?MODULE, Cmds, Env),
                        ?WHENFAIL(begin
                                      %%io:format("S = ~p\nR = ~p\n", [S, Res]),
                                      io:format("S = ~p\nR = ~p\nHist = ~p\n", [S, Res, _Hist]),
                                      %%io:format("Hist=~p\n", [_Hist]),
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
                  _ = brick_simple:set(?TABLE, <<"/foo/debug-A">>,
                                       io_lib:format("~p", [QQQ])),
                  delete_all_max_keys(),
                  _ = brick_simple:delete(?TABLE, list_to_binary(key_prefix())),
                  _ = brick_simple:set(?TABLE, <<"/foo/debug-Z">>,
                                       io_lib:format("~p", [QQQ])),
                  {false, x}
              catch
                  _:_ ->
                      {true, x}
              end
      end, x),
    L = [{make_key(X), []} || X <- lists:seq(1, ?MAX_KEYS)],
    {ok, Ps} = brick_admin:get_table_info(brick_admin, ?TABLE, 10*1000),
    GH = proplists:get_value(ghash, Ps),
    AllChains =
        (GH#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
        (GH#g_hash_r.new_h_desc)#hash_r.healthy_chainlist,
    AllBricks = [B || {_ChainName,Bricks} <- AllChains, B <- Bricks],
    #state{dict = orddict:from_list(L),
           bricks = lists:usort(AllBricks)}.

delete_all_max_keys() ->
    L = [{make_key(X), []} || X <- lists:seq(1, ?MAX_KEYS)],
    [_ = is_ok_notex(catch brick_simple:delete(?TABLE, K)) || {K, _} <- L],
    %% io:format("ALL DELETED\n"),
    ok.

%% command :: (S::symbolic_state()) :: gen(call() | stop)
%% Called in symbolic context.

command(S) ->
    ?LET(Exp, make_exp(S),
         frequency(
           [{20,  {call, ?MODULE, simple_set, [random_key(), random_val(),
                                               Exp, random_mod_flags()]}},
            {20,  {call, ?MODULE, simple_add, [random_key(), random_val(),
                                               Exp, random_mod_flags()]}},
            {20,  {call, ?MODULE, simple_replace, [random_key(), random_val(),
                                                   Exp, random_mod_flags()]}},
            %% We need more gets than the others because they're needed to help
            %% detect consistency errors if/when migration and or brick failures
            %% are happening while we're running.
            {100,  {call, ?MODULE, simple_get, [random_key(), random_get_flags()]}},
            {10,  {call, ?MODULE, simple_delete, [random_key()]}},
            {25,  {call, ?MODULE, simple_do, [list(random_do(S)), random_do_flags()]}},
            %%        {10,  {call, ?MODULE, simple_get_many, [random_many_key(), nat()]}},
            {2,   {call, ?MODULE, trigger_checkpoint, [oneof(S#state.bricks)]}},

            %% 2-tier storage and sync changing isn't good, yet.
            %%        {1,   {call, ?MODULE, set_do_sync, [oneof(S#state.bricks),
            %%                                          oneof([true, false])]}},
            {3,   {call, ?MODULE, scavenge, [oneof(S#state.bricks)]}},
            {3,   {call, ?MODULE, sync_down_the_chain, [oneof(S#state.bricks)]}}

            %% {1,   {call, ?MODULE, crash_brick, [oneof(S#state.bricks)]}},
           ])).

random_do(S) ->
    ?LET(Exp, make_exp(S),
         frequency(
           [
            {20, {simple_set, [random_key(), random_val(), Exp, random_mod_flags()]}},
            {20, {simple_add, [random_key(), random_val(), Exp, random_mod_flags()]}},
            {20, {simple_replace, [random_key(), random_val(), Exp, random_mod_flags()]}},
            %% Hibari temp hack: get can timeout at end of migrations, so we'll
            %%                avoid the problem for now.
            %%        {25, {simple_get, [random_key(), random_get_flags()]}},
            {10, {simple_delete, [random_key()]}}
           ])).

random_do_flags() ->
    %% I think that sync_override is broken.
    [].
%%     ?LET({F1, F2}, {frequency([{10, []}, {5, [{sync_override, bool()}]}]),
%%                   frequency([{10, []}, {5, [ignore_role_bad_idea_delme]}])},
%%        F1 ++ F2).

random_get_flags() ->
    oneof([[], [get_all_attribs]]).

random_mod_flags() ->
%%%oneof([[], [value_in_ram]]).
    [].

random_many_key() ->
    frequency([{1, list_to_binary(key_prefix())}, {3, random_key()}]).

%% precondition(S::symbolic_state(), C::call()) :: bool()
%% Called in symbolic (??) & runtime context.

precondition(_S, {call, _, simple_set, [_Key, _Val, _Exp, _Flags]}) ->
    true;
precondition(_S, {call, _, simple_add, [_Key, _Val, _Exp, _Flags]}) ->
    true;
precondition(_S, {call, _, simple_replace, [_Key, _Val, _Exp, _Flags]}) ->
    true;
precondition(_S, {call, _, simple_get, [_Key, _Flags]}) ->
    true;
precondition(_S, {call, _, simple_delete, [_Key]}) ->
    true;
precondition(_S, {call, _, simple_do, [[], _]}) ->
    false;                                      % Don't do the empty list
precondition(_S, {call, _, simple_do, [List, _]}) ->
    %% Mixing read and write ops in the same do will fail, so avoid them.
    (all_do_list_are(List, read) orelse all_do_list_are(List, write))
    %% NOTE: with the all_same_brick() test, the above 2 tests are redundant.
        andalso
        all_same_brick(List);
precondition(_S, {call, _, simple_get_many, _}) ->
    true;
precondition(_S, {call, _, trigger_checkpoint, _}) ->
    true;
precondition(_S, {call, _, set_do_sync, _}) ->
    true;
precondition(_S, {call, _, scavenge, _}) ->
    true;
precondition(_S, {call, _, sync_down_the_chain, _}) ->
    true;
precondition(_S, {call, _, crash_brick, _}) ->
    true;
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

postcondition2(_S, {call, _, simple_set, [_Key, _Val, _Exp, _Flags]} = _C, R) ->
    case R of
        _ ->
            is_ok(R)
    end;
postcondition2(S, {call, _, simple_add, [Key, _Val, _Exp, _Flags]} = _C, R) ->
    Vs = orddict:fetch(Key, S#state.dict),
    case R of
        %%      %% For brick txn semantics
        %%      {txn_fail, [{_, {key_exists, _TS}}]} ->
        %%          Vs /= [];
        {key_exists, _TS} ->
            has_one_definite_or_maybe_val(Vs);
        {ok, _TS} ->
            has_zero_definite_vals(Vs)
    end;
postcondition2(S, {call, _, simple_replace, [Key, _Val, _Exp, _Flags]} = _C, R) ->
    Vs = orddict:fetch(Key, S#state.dict),
    case R of
        %%      %% For brick txn semantics
        %%      {txn_fail, [{_, key_not_exist}]} ->
        %%          Vs == [];
        key_not_exist ->
            has_zero_definite_vals(Vs);
        {ok, _TS} ->
            has_one_definite_or_maybe_val(Vs)
    end;
postcondition2(S, {call, _, simple_get, [Key, Flags]} = _C, R) ->
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
postcondition2(S, {call, _, simple_delete, [Key]} = _C, R) ->
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
postcondition2(S, {call, _, simple_get_many, [Key, MaxNum]} = _C, R) ->
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

next_state2(S, Result, {call, _, simple_set, [Key, Val, _Exp, _Flags]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of {ok, _TS} -> [Val];
              {'EXIT', _} -> [{maybe_set, Val}|Old]
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, {key_exists, _TS}, {call, _, simple_add, [_Key, _Val, _Exp, _Flags]}) ->
    %% TODO: If QuickCheck had control over the timestamps that we send,
    %%       then we would be able to use _TS to shrink the history list
    %%       in the #state.dict.
    S;
next_state2(S, Result, {call, _, simple_add, [Key, Val, _Exp, _Flags]}) ->
    next_state2(S, Result, {call, ?MODULE, simple_set, [Key, Val, _Exp, _Flags]});

next_state2(S, key_not_exist, {call, _, simple_replace, [_Key, _Val, _Exp, _Flags]}) ->
    %% TODO: If QuickCheck had control over the timestamps that we send,
    %%       then we would be able to use _TS to shrink the history list
    %%       in the #state.dict.
    S;
next_state2(S, Result, {call, _, simple_replace, [Key, Val, _Exp, _Flags]}) ->
    next_state2(S, Result, {call, ?MODULE, simple_set, [Key, Val, _Exp, _Flags]});

next_state2(S, Result, {call, _, simple_get, [Key, _Flags]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of key_not_exist                 -> [];
              {ok, _TS, Val}                -> [Val];
              {ok, _TS, Val, _ExpTime, _Fs} -> [Val];
              {'EXIT', _}                   -> Old
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, Result, {call, _, simple_delete, [Key]}) ->
    Old = orddict:fetch(Key, S#state.dict),
    New = case Result of key_not_exist -> [];
              ok            -> [];
              {'EXIT', _}   -> [maybe_delete|Old]
          end,
    S#state{dict = orddict:store(Key, New, S#state.dict)};

next_state2(S, FailedResult, {call, _, simple_do, [CrudeOps, _]} = C)
  when is_tuple(FailedResult) ->
    %% Make N copies of FailedResult and try again.
    next_state2(S, lists:duplicate(length(CrudeOps), FailedResult), C);
next_state2(S, ResList, {call, _, simple_do, [CrudeOps, _]}) ->
    CrudeRes = lists:zip(CrudeOps, ResList),
    lists:foldl(fun({{simple_get, _Args}, _Res}, St) ->
                        %% Tricky tricky: If we're part of a txn,
                        %% and an earlier part of this txn modified
                        %% the key we're now reading, then we'll read
                        %% the server's current value, *not* the value
                        %% set earlier in this txn!
                        %% (I.e. no dirty read)
                        %% Therefore, we don't call next_state2()
                        %% because next_state2() for simple_read will
                        %% pop items off our dict's queue of values.
                        St;
                   ({{Name, Args}, Res}, St) ->
                        S2 = next_state2(St, Res, {call, ?MODULE, Name, Args}),
                        S2
                end, S, CrudeRes);

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

simple_set(Key, Val, Exp, Flags) ->
    catch brick_simple:set(?TABLE, Key, Val, Exp, Flags, 9000).

simple_add(Key, Val, Exp, Flags) ->
    catch brick_simple:add(?TABLE, Key, Val, Exp, Flags, 9000).

simple_replace(Key, Val, Exp, Flags) ->
    catch brick_simple:replace(?TABLE, Key, Val, Exp, Flags, 9000).

simple_get(Key, Flags) ->
    %% Use much smaller timeout here: if we timeout, no huge deal.
    %% It's more important to avoid waiting.
    catch brick_simple:get(?TABLE, Key, Flags, 2000).

simple_delete(Key) ->
    catch brick_simple:delete(?TABLE, Key, 9000).

simple_do(CrudeOps, DoFlags) ->
    DoList = lists:map(
               fun({simple_set, [K, V, Exp, Flags]}) ->
                       brick_server:make_set(K, V, Exp, Flags);
                  ({simple_add, [K, V, Exp, Flags]}) ->
                       brick_server:make_add(K, V, Exp, Flags);
                  ({simple_replace, [K, V, Exp, Flags]}) ->
                       brick_server:make_replace(K, V, Exp, Flags);
                  ({simple_get, [K, Flags]}) ->
                       brick_server:make_get(K, Flags);
                  ({simple_delete, [K]}) ->
                       brick_server:make_delete(K)
               end, CrudeOps),
    catch brick_simple:do(?TABLE, DoList, DoFlags, 9000).

simple_get_many(Key, MaxNum) ->
    catch brick_simple:get_many(?TABLE, Key, MaxNum,
                                [witness,
                                 {binary_prefix, list_to_binary(key_prefix())}]).

is_ok(ok)                      -> true;
is_ok({'EXIT', {shutdown, _}}) -> true;
is_ok({'EXIT', {noproc, _}})   -> true;         % Would be false in ideal world
is_ok({'EXIT', {timeout, _}})  -> false;        % Is false in an ideal world
is_ok(X)                       -> X.

is_ok_notex(ok) ->            true;
is_ok_notex(key_not_exist) -> true;
is_ok_notex(X) ->             X.

expect_equal(_Tag, X, X) ->
    ok;
expect_equal(Tag, Got, Expected) ->
    io:format("expect_equal: ~p: got ~p, expected ~p\n", [Tag, Got, Expected]),
    exit({Tag, got, Got, expected, Expected}).

calc_state_usage(S) ->
    Hs = orddict:to_list(S#state.dict),
    lists:foldl(
      fun({_K, []}, Acc) ->
              Acc;
         ({_K, [H|_]}, {Items, Bytes}) ->
              {Items + 1, Bytes + gmt_util:io_list_len(H)}
      end, {0, 0}, Hs).

calc_actual_usage(QRoot) ->
    {ok, {All, false}} = brick_simple:get_many(?TABLE, QRoot, ?MAX_KEYS+70,
                                               [{binary_prefix, QRoot}]),
    Vals = [V || {_K, _TS, V} <- All],
    Items = length(Vals),
    Bytes = lists:foldl(fun(X, Sum) -> size(X) + Sum end, 0, Vals),
    {Items, Bytes}.

trigger_checkpoint({BrickName, Node}) ->
    io:format("c"),
    catch brick_server:checkpoint(BrickName, Node, [silent]).

set_do_sync({BrickName, Node}, Bool) ->
    Fmt = if Bool -> "S"; true -> "s" end, io:format(Fmt),
    catch brick_server:set_do_sync({BrickName, Node}, Bool).

scavenge({_BrickName, _Node}) ->
    io:format("v"),
    catch gmt_hlog_common:start_scavenger_commonlog(
            [{skip_live_percentage_greater_than, 0}, silent]).

sync_down_the_chain({BrickName, Node}) ->
    io:format("Y"),
    catch brick_server:sync_down_the_chain(BrickName, Node, []).

crash_brick({BrickName, Node}) ->
    catch brick_server:stop({BrickName, Node}).

all_do_list_are(DoList, Type) ->
    lists:all(fun(Do) -> classify_do(Do) == Type end, DoList).

classify_do({simple_set, _})     -> write;
classify_do({simple_add, _})     -> write;
classify_do({simple_replace, _}) -> write;
classify_do({simple_delete, _})  -> write;
classify_do({simple_get, _})     -> read.

scrape_keys(List) ->
    [hd(Args) || {_Simple_do_name, Args} <- List].

all_same_brick(List) ->
    X = [begin
             RdWr = classify_do(Do),
             Key = hd(element(2, Do)),
             brick_simple:find_the_brick(?TABLE, Key, RdWr)
         end || Do <- List],
    length(X) == 1.

chain_get_role(BrickName, BrickNode) ->
    case (catch brick_server:chain_get_role(BrickName, BrickNode)) of
        {'EXIT', _} ->                          % crashed/shutdown race
            undefined;
        Res ->
            Res
    end.

%%
%% There's a problem with the check for the FOO_store vs. FOO_exp
%% tables: updates to the former are paused during checkpoint
%% operations.  So, if we get a false, sleep for a sec to allow any
%% checkpoints to finish, then check again.
%%
%% NOTE: This is a nasty hack that makes assumptions about table
%% names, etc etc.
%%

ets_table_sizes_match_p(S) ->
    SleepAmt = 100,
    true = gmt_loop:do_while(
             fun(N) when N > 5000 ->
                     ets_table_sizes_match_p2(b, S),
                     {false, N};
                (N) ->
                     case ets_table_sizes_match_p2(a, S) of
                         true ->
                             {false, true};
                         false ->
                             timer:sleep(SleepAmt),
                             {true, N + SleepAmt}
                     end
             end, 0).

ets_table_sizes_match_p2(_AorB, _S) ->
    L11s = (catch [Exp || {_, _, _, _, Exp} <- ets:tab2list(tab1_ch1_b1_store)]),
    L11e = (catch ets:tab2list(tab1_ch1_b1_exp)),
    L12s = (catch [Exp || {_, _, _, _, Exp} <- ets:tab2list(tab1_ch1_b2_store)]),
    L12e = (catch ets:tab2list(tab1_ch1_b2_exp)),
    Len11s = (catch length(L11s)),
    Len11e = (catch length(L11e)),
    Len12s = (catch length(L12s)),
    Len12e = (catch length(L12e)),
    Res = if not is_integer(Len11s) ->
                  if not is_integer(Len12s) ->
                          %% Neither table exists, so avoid stopping
                          %% QuickCheck by returning true.
                          true;
                     true ->
                          Len12s == Len12e
                  end;
             not is_integer(Len12s) ->
                  if not is_integer(Len11s) ->
                          false;
                     true ->
                          Len11s == Len11e
                  end;
             true ->
                  Len11s == Len11e andalso Len12s == Len12e
          end,
    if Res == false, _AorB == b ->
            io:format("ERR: ~p: Sizes: ~P ~P ~P ~P\n",
                      [time(), Len11s, 1, Len11e, 1, Len12s, 1, Len12e, 1]),
            io:format("ERR: ~p\n~p\n~p\n~p\n",
                      [L11s, L11e, L12s, L12e]);
       true ->
            ok
    end,
    Res.

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

stopfile() -> "./QC-IS-RUNNING".

stopfile_exists() ->
    case file:read_file(stopfile()) of {ok, _} -> true;
        _       -> false
    end.

wrap_with_stopfile(Num) when is_integer(Num) ->
    wrap_with_stopfile(
      fun() -> ?GMTQC:quickcheck(noshrink(numtests(Num, prop_simple1_noproc_ok()))) end);
wrap_with_stopfile(Fun) when is_function(Fun) ->
    file:write_file(stopfile(), <<>>),
    Res = Fun(),
    file:delete(stopfile()),
    Res.

%% Assumes using tab1 and brick naming convention tab1_ch1_b1 and tab1_ch1_b2.
change_chain_len_while_qc_running() ->
    gmt_loop:do_while(
      fun(DesiredLen) ->
              case {stopfile_exists(), DesiredLen} of
                  {false, _} ->
                      {false, done};
                  {true, 1} ->
                      case random:uniform(2) of
                          1 ->
                              io:format("1a"),
                              brick_admin:change_chain_length(tab1_ch1, [{tab1_ch1_b1, node()}]);
                          2 ->
                              io:format("1b"),
                              brick_admin:change_chain_length(tab1_ch1, [{tab1_ch1_b2, node()}])
                      end,
                      timer:sleep(4*1000),
                      {true, 2};
                  {true, 2} ->
                      io:format("2"),
                      brick_admin:change_chain_length(tab1_ch1, [{tab1_ch1_b1, node()}, {tab1_ch1_b2, node()}]),
                      timer:sleep(4*1000),
                      {true, 1}
              end
      end, 1).

change_num_chains_while_qc_running() ->
    gmt_loop:do_while(
      fun(_Acc) ->
              case stopfile_exists() of
                  false ->
                      {false, done};
                  true ->
                      change_num_chains(),
                      timer:sleep(100),
                      {true, ignored}
              end
      end, dontcare).

new_random_lh(Tab) ->
    {ok, [{_, OldL}|_]} = brick_admin:get_table_chain_list(Tab),
    OldLen = length(OldL),
    NumChains = 4,
    NewChains0 = brick_admin:make_chain_description(
                   Tab, OldLen, lists:duplicate(NumChains, node())),
    NewChains = case [X || X <- NewChains0, random:uniform(100) < 50] of
                    []   -> NewChains0;
                    Else -> Else
                end,
    NewWeights = [{Ch, 100} || {Ch, _} <- NewChains],
    {ok, OldGH} = brick_admin:get_gh_by_table(Tab),
    OldProps = lists:keydelete(new_chainweights, 1,
                               brick_hash:chash_extract_old_props(OldGH)),
    brick_hash:chash_init(via_proplist, NewChains,
                          [{new_chainweights, NewWeights}|OldProps]).

change_num_chains() ->
    change_num_chains(tab1).

change_num_chains(Tab) ->
    LH = new_random_lh(Tab),
    brick_admin:start_migration(Tab, LH,
                                [{max_keys_per_iter, random:uniform(5)}]).

-endif. %% -ifdef(GMTQC).
