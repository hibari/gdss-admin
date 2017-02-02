%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2017 Hibari developers.  All rights reserved.
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

-ifdef(QC).

-eqc_group_commands(false).
-include_lib("qc/include/qc_statem.hrl").

-export([run/0]).
-compile(export_all).

-include("brick_hash.hrl").
-include("brick_public.hrl").

%% qc_statem Callbacks
%% -behaviour(qc_statem).
-export([command/1]).
-export([initial_state/0, initial_state/1, next_state/3, invariant/1, precondition/2, postcondition/3]).
-export([init/0, init/1, stop/2, aggregate/1]).


%%%% -define(MAX_KEYS, 10).
-define(MAX_KEYS, 4).
-define(TABLE, tab1).

-type proplist() :: proplists:proplist().
-type orddict() :: term().

-type key() :: iolist() | string() | binary().
-type val() :: binary().
-type exp_time() :: integer().
-type timestamp() :: integer().

-type do_flag() :: witness.
-type get_flag() :: get_all_attribs.
-type mod_flag() :: {exp_time_directive, keep | replace}
                  | {attrib_directive, keep | replace}.

-type op_flag() :: do_flag() | get_flag() | mod_flag().
-type attribute() :: {atom() | term()}.

-type write_operation() :: simple_set | simple_add | simple_replace | simple_rename
                         | simple_delete.
-type read_opetation() :: simple_get | simple_get_many.
-type do_operation() :: simple_do.
-type brick_operation() :: trigger_checkpoint | set_do_sync | scavenge
                         | sync_down_the_chain | crash_brick.

-type qc_command() :: {call, module(),
                       write_operation() | read_opetation() | do_operation() | brick_operation(),
                       [term()]}.
-type do_command() :: {write_operation() | read_opetation() | do_operation() | brick_operation(),
                       [term()]}.

-record(state, {
          step = 1 :: integer(),
          exp_times :: orddict(),  %% key :: term(), val :: integer()
          values :: orddict(),     %% key :: term(), val :: [term()] of possible vals
          attributes :: orddict(), %% key :: term(), val :: [attribute()]
          bricks                   %% [brick_names()]
         }).
-type state() :: #state{}.
-type symbolic_state() :: state().
-type dynamic_state() :: state().

%% run from eunit
eunit_test_() ->
    qc:eunit_module(?MODULE, 3000).

-spec init() -> ok.
init() ->
    ok.

-spec init(#state{}) -> ok.
init(_State) ->
    ok.

-spec stop(#state{}, #state{}) -> ok.
stop(_State0, _State) ->
    ok.

run() ->
    run(500).

run(NumTests) ->
    qc_statem:qc_run(?MODULE, NumTests, []).

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
    error_logger:delete_report_handler(error_logger_tty_h),
    setup(),
    timer:sleep(2000),
    io:format("\n\nHibari: get_many() has been commented out from command()\n\n"),
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

setup() ->
    %% true to enable sasl output
    X = brick_eunit_utils:setup_and_bootstrap(false),
    X.

teardown(X) ->
    brick_eunit_utils:teardown(X),
    ok.

setup_noop() ->
    %% disable sasl output
    error_logger:delete_report_handler(error_logger_tty_h),
    ok.

teardown_noop(_X) ->
    ok.

-spec initial_state() -> #state{}.
initial_state() ->
    initial_state([]).

-spec initial_state(proplist()) -> #state{}.
initial_state(_Options) ->
    QQQ = gmt_time_otp18:timestamp(),
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
    ExpTime = [{to_binary(make_key_iolist(X)), undefined} || X <- lists:seq(1, ?MAX_KEYS)],
    Values  = [{to_binary(make_key_iolist(X)), []}        || X <- lists:seq(1, ?MAX_KEYS)],
    Attribs = [{to_binary(make_key_iolist(X)), undefined} || X <- lists:seq(1, ?MAX_KEYS)],
    {ok, Ps} = brick_admin:get_table_info(brick_admin, ?TABLE, 10*1000),
    GH = proplists:get_value(ghash, Ps),
    AllChains =
        (GH#g_hash_r.current_h_desc)#hash_r.healthy_chainlist ++
        (GH#g_hash_r.new_h_desc)#hash_r.healthy_chainlist,
    AllBricks = [B || {_ChainName,Bricks} <- AllChains, B <- Bricks],
    #state{exp_times  = orddict:from_list(ExpTime),
           values     = orddict:from_list(Values),
           attributes = orddict:from_list(Attribs),
           bricks     = lists:usort(AllBricks)}.

delete_all_max_keys() ->
    L = [{to_binary(make_key_iolist(X)), []} || X <- lists:seq(1, ?MAX_KEYS)],
    [_ = is_ok_notex(catch brick_simple:delete(?TABLE, K)) || {K, _} <- L],
    %% io:format("ALL DELETED\n"),
    ok.


%% Called in symbolic context.
-spec command(symbolic_state()) -> qc_command().
command(S) ->
    ?LET(Exp, make_exp(S),
         frequency(
           [{20,  {call, ?MODULE, simple_set,     [random_key(), random_val(), Exp,
                                                   random_mod_flags(simple_set),
                                                   random_attributes()]}},
            {20,  {call, ?MODULE, simple_add,     [random_key(), random_val(), Exp,
                                                   random_mod_flags(simple_add),
                                                   random_attributes()]}},
            {20,  {call, ?MODULE, simple_replace, [random_key(), random_val(), Exp,
                                                   random_mod_flags(simple_replace),
                                                   random_attributes()]}},
            {20,  {call, ?MODULE, simple_rename,  [random_key(), random_key(), Exp,
                                                   random_mod_flags(simple_rename),
                                                   random_attributes()]}},
            %% We need more gets than the others because they're needed to help
            %% detect consistency errors if/when migration and or brick failures
            %% are happening while we're running.
            {100, {call, ?MODULE, simple_get,    [random_key(), random_get_flags()]}},
            {10,  {call, ?MODULE, simple_delete, [random_key()]}},
            {25,  {call, ?MODULE, simple_do,     [list(random_do(S)), random_do_flags()]}},
            %% {10,  {call, ?MODULE, simple_get_many, [random_many_key(), nat()]}},
            {2,   {call, ?MODULE, trigger_checkpoint, [oneof(S#state.bricks)]}},

            %% 2-tier storage and sync changing isn't good, yet.
            %% {1,   {call, ?MODULE, set_do_sync, [oneof(S#state.bricks),
            %%                                          oneof([true, false])]}},
            {3,   {call, ?MODULE, scavenge, [oneof(S#state.bricks)]}},
            {3,   {call, ?MODULE, sync_down_the_chain, [oneof(S#state.bricks)]}}

            %% {1,   {call, ?MODULE, crash_brick, [oneof(S#state.bricks)]}},
           ])).

-spec random_do(symbolic_state()) -> do_command().
random_do(S) ->
    ?LET(Exp, make_exp(S),
         frequency(
           [
            {20, {simple_set,     [random_key(), random_val(), Exp,
                                   random_mod_flags(simple_set), random_attributes()]}},
            {20, {simple_add,     [random_key(), random_val(), Exp,
                                   random_mod_flags(simple_add), random_attributes()]}},
            {20, {simple_replace, [random_key(), random_val(), Exp,
                                   random_mod_flags(simple_replace), random_attributes()]}},
            {20, {simple_rename,  [random_key(), random_key(), Exp,
                                   random_mod_flags(simple_rename), random_attributes()]}},
            %% Hibari temp hack: get can timeout at end of migrations, so we'll
            %%                avoid the problem for now.
            %%        {25, {simple_get, [random_key(), random_get_flags()]}},
            {10, {simple_delete,  [random_key()]}}
           ])).

-spec random_do_flags() -> [do_flag()].
random_do_flags() ->
    frequency([{10, []}, {5, [witness]}]).
    %% I think that sync_override is broken.
    %% [].
%%     ?LET({F1, F2}, {frequency([{10, []}, {5, [{sync_override, bool()}]}]),
%%                   frequency([{10, []}, {5, [ignore_role_bad_idea_delme]}])},
%%        F1 ++ F2).

-spec random_get_flags() -> [get_flag()].
random_get_flags() ->
    oneof([[], [get_all_attribs]]).

-spec random_mod_flags(write_operation()) -> [mod_flag()].
random_mod_flags(Op)
  when Op =:= simple_set;
       Op =:= simple_replace;
       Op =:= simple_rename ->
  ?LET(Flags, [ oneof([ [], [{exp_time_directive, keep}], [{exp_time_directive, replace}] ]),
                oneof([ [], [{attrib_directive, keep}], [{attrib_directive, replace}] ]) ],
       lists:flatten(Flags));
random_mod_flags(_) ->
    [].

-spec random_attributes() -> [attribute()].
random_attributes() ->
    oneof([ [], [{oneof([attrib1, attrib2, attrib3]),
                  oneof(["val1", "val2", "val3"])}] ]).

-spec random_many_key() -> binary().
random_many_key() ->
    frequency([{1, list_to_binary(key_prefix())}, {3, random_key()}]).

%% Called in symbolic (??) & runtime context.
-spec precondition(S::symbolic_state(), C::qc_command()) -> boolean().
precondition(_S, {call, _, simple_set, [_Key, _Val, _Exp, _Flags, _Attribs]}) ->
    true;
precondition(_S, {call, _, simple_add, [_Key, _Val, _Exp, _Flags, _Attribs]}) ->
    true;
precondition(_S, {call, _, simple_replace, [_Key, _Val, _Exp, _Flags, _Attribs]}) ->
    true;
precondition(_S, {call, _, simple_rename, [_OldKey, _NewKey, _Exp, _Flags, _Attribs]}) ->
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

%% Called in runtime context.
-spec postcondition(S::dynamic_state(), C::qc_command(), R::term()) -> boolean().
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

-spec postcondition2(S::dynamic_state(), C::qc_command(), R::term()) -> boolean().
postcondition2(_S, {call, _, simple_set, [_Key, _Val, _Exp, _Flags, _Attribs]} = _C, R) ->
    case R of
        _ ->
            is_ok(R)
    end;
postcondition2(S, {call, _, simple_add, [Key, _Val, _Exp, _Flags, _Attribs]} = _C, R) ->
    BinKey = to_binary(Key),
    Vs = orddict:fetch(BinKey, S#state.values),
    case R of
        %%      %% For brick txn semantics
        %%      {txn_fail, [{_, {key_exists, _TS}}]} ->
        %%          Vs /= [];
        {key_exists, _TS} ->
            has_one_definite_or_maybe_val(Vs);
        {ok, _TS} ->
            has_zero_definite_vals(Vs)
    end;
postcondition2(S, {call, _, simple_replace, [Key, _Val, _Exp, _Flags, _Attribs]} = _C, R) ->
    BinKey = to_binary(Key),
    Vs = orddict:fetch(BinKey, S#state.values),
    case R of
        %%      %% For brick txn semantics
        %%      {txn_fail, [{_, key_not_exist}]} ->
        %%          Vs == [];
        key_not_exist ->
            has_zero_definite_vals(Vs);
        {ok, _TS} ->
            has_one_definite_or_maybe_val(Vs)
    end;
postcondition2(S, {call, _, simple_rename, [OldKey, NewKey, _Exp, _Flags, _Attribs]} = _C, R) ->
    BinOldKey = to_binary(OldKey),
    OldVs = orddict:fetch(BinOldKey, S#state.values),
    case R of
        key_not_exist ->
            BinOldKey =:= to_binary(NewKey) orelse has_zero_definite_vals(OldVs);
        {ok, _TS} ->
            has_one_definite_or_maybe_val(OldVs)
    end;
postcondition2(S, {call, _, simple_get, [Key, Flags]} = _C, R) ->
    BinKey = to_binary(Key),
    Vs = orddict:fetch(BinKey, S#state.values),
    case R of
        key_not_exist ->
            has_zero_definite_vals(Vs);
        {ok, _TS, Val} ->
            proplists:get_value(get_all_attribs, Flags) =:= undefined
                andalso val_is_a_definite_or_maybe(Val, Vs);
        %% with get_all_attribs flag
        {ok, _TS, Val, ExpTime, Attributes} ->
            proplists:get_value(get_all_attribs, Flags) =/= undefined
                andalso val_is_a_definite_or_maybe(Val, Vs)
                andalso check_exp_time(BinKey, ExpTime, S)
                andalso check_attributes(BinKey, Attributes, S);
        {'EXIT', {timeout, _}} ->
            true                               % Hibari temp hack?
    end;
postcondition2(S, {call, _, simple_delete, [Key]} = _C, R) ->
    BinKey = to_binary(Key),
    Vs = orddict:fetch(BinKey, S#state.values),
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
    AllKeys0 = [to_binary(K) || {K, V} <- orddict:to_list(S#state.values),
                                K > Key, V /= []],
    {AllKeys, Rest} = lists:split(if MaxNum > length(AllKeys0) ->
                                          length(AllKeys0);
                                     true ->
                                          MaxNum
                                  end, AllKeys0),
    case R of
        {ok, {Rs, Bool}} ->
            RsKs = [to_binary(K) || {K, _TS} <- Rs],
            %% Since we don't know what's beyond the end of our limited range,
            %% we can't test the correctness of Bool 100% completely.
            RsKs == AllKeys
                andalso ((MaxNum == 0 andalso Bool == true)
                         orelse (Bool == true andalso Rest /= [])
                         orelse (Bool == false andalso AllKeys == []));
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

%% Called in symbolic & runtime context.
-spec next_state(S::symbolic_state(), R::term(), C::qc_command()) -> symbolic_state().
next_state(S, {var, _}, _C) ->
    %% BEWARE, this is big, big-time QuickCheck cheating.  It will
    %% really screw up QC's shrinking, but I'm more interested in
    %% finding weird race conditions and diagnosing them via gmt_elog
    %% tracing than worrying about deterministic shrinking.
    S#state{step = S#state.step + 1};
next_state(S, R, C) ->
    NewS = next_state2(S, R, C),
    NewS#state{step = S#state.step + 1}.

-spec next_state2(S::symbolic_state(), R::term(), C::qc_command()) -> symbolic_state().
next_state2(S, Result, {call, _, simple_set, [Key, Val, Exp, Flags, Attribs]}) ->
    BinKey = to_binary(Key),
    OldExp = orddict:fetch(BinKey, S#state.exp_times),
    OldAttribs = orddict:fetch(BinKey, S#state.attributes),
    OldVal = orddict:fetch(BinKey, S#state.values),
    NewVal = case Result of {ok, _TS} -> [Val];
                 {'EXIT', _} -> [{maybe_set, Val}|OldVal]
             end,
    S#state{values =
                orddict:store(BinKey, NewVal, S#state.values),
            exp_times =
                orddict:store(BinKey,
                              apply_exp_time_directive(Flags, Exp, OldExp, replace),
                              S#state.exp_times),
            attributes =
                orddict:store(BinKey,
                              apply_attrib_directive(Flags, Attribs, OldAttribs, replace),
                              S#state.attributes) };
next_state2(S, {key_exists, _TS}, {call, _, simple_add, [_Key, _Val, _Exp, _Flags, _Attribs]}) ->
    %% TODO: If QuickCheck had control over the timestamps that we send,
    %%       then we would be able to use _TS to shrink the history list
    %%       in the #state.values.
    S;
next_state2(S, Result, {call, _, simple_add, [Key, Val, _Exp, _Flags, _Attribs]}) ->
    next_state2(S, Result, {call, ?MODULE, simple_set, [Key, Val, _Exp, _Flags, _Attribs]});
next_state2(S, key_not_exist, {call, _, simple_replace, [_Key, _Val, _Exp, _Flags, _Attribs]}) ->
    %% TODO: If QuickCheck had control over the timestamps that we send,
    %%       then we would be able to use _TS to shrink the history list
    %%       in the #state.values.
    S;
next_state2(S, Result, {call, _, simple_replace, [Key, Val, _Exp, _Flags, _Attribs]}) ->
    next_state2(S, Result, {call, ?MODULE, simple_set, [Key, Val, _Exp, _Flags, _Attribs]});
next_state2(S, key_not_exist, {call, _, simple_rename, [_OldKey, _NewKey, _Exp, _Flags, _Attribs]}) ->
    %% TODO: If QuickCheck had control over the timestamps that we send,
    %%       then we would be able to use _TS to shrink the history list
    %%       in the #state.values.
    S;
next_state2(S, Result, {call, _, simple_rename, [OldKey, NewKey, Exp, Flags, Attribs]}) ->
    case Result of
        {ok, _TS} ->
            BinOldKey = to_binary(OldKey),
            BinNewKey = to_binary(NewKey),
            OldExp = orddict:fetch(BinOldKey, S#state.exp_times),
            OldAttribs = orddict:fetch(BinOldKey, S#state.attributes),
            Vals = orddict:fetch(BinOldKey, S#state.values),
            S2 = S#state{values =
                             orddict:store(BinNewKey, [hd(Vals)], S#state.values),
                         exp_times =
                             orddict:store(BinNewKey,
                                           apply_exp_time_directive(Flags, Exp, OldExp, keep),
                                           S#state.exp_times),
                         attributes =
                             orddict:store(BinNewKey,
                                           apply_attrib_directive(Flags, Attribs, OldAttribs, keep),
                                           S#state.attributes) },
            next_state2(S2, ok, {call, ?MODULE, simple_delete, [BinOldKey]});
        {'EXIT', _} ->
            S
    end;
next_state2(S, Result, {call, _, simple_get, [Key, _Flags]}) ->
    BinKey = to_binary(Key),
    Old = orddict:fetch(BinKey, S#state.values),
    New = case Result of
              key_not_exist                 -> [];
              {ok, _TS, Val}                -> [Val];
              {ok, _TS, Val, _ExpTime, _Fs} -> [Val];
              {'EXIT', _}                   -> Old
          end,
    S#state{values = orddict:store(BinKey, New, S#state.values)};
next_state2(S, Result, {call, _, simple_delete, [Key]}) ->
    BinKey = to_binary(Key),
    Old = orddict:fetch(BinKey, S#state.values),
    New = case Result of key_not_exist -> [];
              ok            -> [];
              {'EXIT', _}   -> [maybe_delete|Old]
          end,
    S#state{exp_times =
                orddict:store(BinKey, undefined, S#state.exp_times),
            values =
                orddict:store(BinKey, New, S#state.values),
            attributes =
                orddict:store(BinKey, undefined, S#state.attributes)};
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

-spec invariant(#state{}) -> boolean().
invariant(_S) ->
    true.

-spec aggregate([{integer(), term(), term(), #state{}}])
               -> [{atom(), integer(), term()}].
aggregate(L) ->
    [ {Cmd,length(Args),filter_reply(Reply)} || {_N,{set,_,{call,_,Cmd,Args}},Reply,_State} <- L ].

filter_reply({'EXIT',{Err,_}}) ->
    {error,Err};
filter_reply(_) ->
    ok.

-spec random_key() -> iolist() | string() | binary().
random_key() ->
    ?LET(I, choose(1, ?MAX_KEYS),
         make_key(I)).

-spec make_key(non_neg_integer()) -> key().
make_key(I) ->
    oneof([make_key_iolist(I),                 %% iolist()
           lists:flatten(make_key_iolist(I)),  %% string()
           list_to_binary(make_key_iolist(I))  %% binary()
          ]).

-spec make_key_iolist(non_neg_integer()) -> iolist().
make_key_iolist(I) ->
    io_lib:format("~s~4.4.0w", [key_prefix(), I]).

-spec key_prefix() -> key().
key_prefix() ->
    "/foo/bar/".

-spec make_exp(state()) -> exp_time().
make_exp(S) ->
    NowX = gmt_time_otp18:erlang_system_time(micro_seconds),
    %% TODO: FIXME: This should read (NowX div 1000) or
    %% just NowX with erlang_system_time(milli_seconds).
    %% https://github.com/hibari/gdss-admin/issues/11
    (NowX * 1000) + S#state.step.

-spec apply_exp_time_directive([op_flag()],
                               NewExpTime::exp_time(), OldExpTime::exp_time(),
                               Default:: keep | replace) -> exp_time().
apply_exp_time_directive(_Flags, NewExpTime, undefined, _Default) ->
    NewExpTime;
apply_exp_time_directive(Flags, NewExpTime, OldExpTime, Default) ->
    case proplists:get_value(exp_time_directive, Flags) of
        undefined when Default =:= keep ->
            OldExpTime;
        undefined when Default =:= replace ->
            NewExpTime;
        keep ->
            OldExpTime;
        replace ->
            NewExpTime
    end.

-spec apply_attrib_directive([op_flag()],
                             NewAttribs::[attribute()], OldAttribs::[attribute()],
                             Default:: keep | replace) -> [attribute()].
apply_attrib_directive(_Flags, NewAttribs, undefined, _Default) ->
    NewAttribs;
apply_attrib_directive(Flags, NewAttribs, OldAttribs, Default) ->
    case proplists:get_value(attrib_directive, Flags) of
        undefined when Default =:= keep ->
            apply_attrib_directive1(NewAttribs, OldAttribs);
        undefined when Default =:= replace ->
            NewAttribs;
        keep ->
            apply_attrib_directive1(NewAttribs, OldAttribs);
        replace ->
            NewAttribs
    end.

-spec apply_attrib_directive1(NewAttribs::[attribute()], OldAttribs::[attribute()]) ->
                                     [attribute()].
apply_attrib_directive1([], OldAttribs) ->
    OldAttribs;
apply_attrib_directive1(NewAttribs, []) ->
    NewAttribs;
apply_attrib_directive1(NewAttribs, OldAttribs) ->
    F = fun({Key, _}=Attrib, Acc) ->
                gb_trees:enter(Key, Attrib, Acc);
           (Attrib, Acc) when is_atom(Attrib) ->
                gb_trees:enter(Attrib, Attrib, Acc)
        end,
    MergedAttribs0 = lists:foldl(F, gb_trees:empty(), OldAttribs),
    MergedAttribs1 = lists:foldl(F, MergedAttribs0, NewAttribs),
    gb_trees:values(MergedAttribs1).

-spec random_val() -> val().
%% @TODO(tatsuya6502) Generate iolist and string. Check the val in postcondition.
random_val() ->
    oneof([<<>>,
           ?LET({I, Char}, {nat(), choose($a, $z)},
                list_to_binary(io_lib:format("v ~s",
                                             [lists:duplicate(I + 1, Char)])))]).

-spec to_binary(binary() | string()) -> binary().
to_binary(Bin) when is_binary(Bin) ->
    Bin;
to_binary(List) when is_list(List) ->
    list_to_binary(List).

-spec check_exp_time(key(), exp_time(), state()) -> boolean().
check_exp_time(BinKey, Exp, #state{exp_times=ExpDict}) ->
    ExpectedExp = orddict:fetch(BinKey, ExpDict),  %% can be undefined
    Exp =:= ExpectedExp.

-spec check_attributes(key(), [attribute()], state()) -> boolean().
check_attributes(BinKey, Attributes, #state{attributes=AttribDict}) ->
    SortedAttributes = lists:keysort(1, lists:keydelete(val_len, 1, Attributes)),
    ExpectedAttributes = orddict:fetch(BinKey, AttribDict),
    SortedAttributes =:= ExpectedAttributes.

-spec simple_set(key(), val(), exp_time(), [op_flag()], [attribute()]) ->
                        {ok, timestamp()} | {error, term()}.
simple_set(Key, Val, Exp, Flags, Attribs) ->
    catch brick_simple:set(?TABLE, Key, Val, Exp, Flags ++ Attribs, 9000).

simple_add(Key, Val, Exp, Flags, Attribs) ->
    catch brick_simple:add(?TABLE, Key, Val, Exp, Flags ++ Attribs, 9000).

simple_replace(Key, Val, Exp, Flags, Attribs) ->
    catch brick_simple:replace(?TABLE, Key, Val, Exp, Flags ++ Attribs, 9000).

simple_rename(OldKey, NewKey, Exp, Flags, Attribs) ->
    catch brick_simple:rename(?TABLE, OldKey, NewKey, Exp, Flags ++ Attribs, 9000).

simple_get(Key, Flags) ->
    %% Use much smaller timeout here: if we timeout, no huge deal.
    %% It's more important to avoid waiting.
    catch brick_simple:get(?TABLE, Key, Flags, 2000).

simple_delete(Key) ->
    catch brick_simple:delete(?TABLE, Key, 9000).

simple_do(CrudeOps, DoFlags) ->
    DoList = lists:map(
               fun({simple_set, [K, V, Exp, Flags, Attribs]}) ->
                       brick_server:make_set(K, V, Exp, Flags ++ Attribs);
                  ({simple_add, [K, V, Exp, Flags, Attribs]}) ->
                       brick_server:make_add(K, V, Exp, Flags ++ Attribs);
                  ({simple_replace, [K, V, Exp, Flags, Attribs]}) ->
                       brick_server:make_replace(K, V, Exp, Flags ++ Attribs);
                  ({simple_rename, [OldK, NewK, Exp, Flags, Attribs]}) ->
                       brick_server:make_rename(OldK, NewK, Exp, Flags ++ Attribs);
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
is_ok({ok, _TS})               -> true;
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
    Hs = orddict:to_list(S#state.values),
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
classify_do({simple_rename, _})  -> write;
classify_do({simple_delete, _})  -> write;
classify_do({simple_get, _})     -> read.

scrape_keys(List) ->
    [hd(Args) || {_Simple_do_name, Args} <- List].

all_same_brick(List) ->
    X = [begin
             RdWr = classify_do(Do),
             Key = hd(element(2, Do)),
             brick_simple_client:find_the_brick(?TABLE, Key, RdWr)
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
    GetExp = fun({_, _, _, _, Exp})    -> Exp;
                ({_, _, _, _, Exp, _}) -> Exp
             end,
    L11s = (catch lists:map(GetExp, ets:tab2list(tab1_ch1_b1_store))),
    L11e = (catch ets:tab2list(tab1_ch1_b1_exp)),
    L12s = (catch lists:map(GetExp, ets:tab2list(tab1_ch1_b2_store))),
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

%% wrap_with_stopfile(Num) when is_integer(Num) ->
%%     wrap_with_stopfile(
%%       fun() -> ?QC:quickcheck(noshrink(numtests(Num, prop_simple1_noproc_ok()))) end);
%% wrap_with_stopfile(Fun) when is_function(Fun) ->
%%     file:write_file(stopfile(), <<>>),
%%     Res = Fun(),
%%     file:delete(stopfile()),
%%     Res.

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

-endif. %% -ifdef(QC).
