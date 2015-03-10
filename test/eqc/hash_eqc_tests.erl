%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2015 Hibari developers.  All rights reserved.
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
%%% File    : hash_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(hash_eqc_tests).

-ifdef(QC).

-include_lib("qc/include/qc.hrl").

-compile(export_all).

run() ->
    run(30000).

run(NumTests) ->
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

%%-define(MARGIN, 0.0000000000000001). % fails
-define(MARGIN, 0.00000000000001).   % works

prop_1() ->
    ?FORALL({FloatMap, Weights},
            {gen_weight_list(true), gen_weight_list(false)},
            begin
                Res = brick_hash:chash_make_float_map(FloatMap, Weights),
                Sum = sum_all_weights(normalize(Res)),
                Zip = lists:zip(normalize(Weights), normalize(Res)),
                within_margin(Sum, ?MARGIN, 1.0) andalso
                    [true] == lists:usort(
                                [within_margin(Wt, ?MARGIN, Target) ||
                                    {{_Ch1, Target}, {_Ch2, Wt}} <- Zip])
            end).

prop_1(N_Times) ->
    numtests(N_Times, prop_1()).

prop_2() ->
    ?FORALL({FloatMap, Weights},
            {gen_weight_list(true), gen_weight_list(false)},
            begin
                FM = brick_hash:chash_make_float_map(FloatMap, Weights),
                NF = brick_hash:chash_float_map_to_nextfloat_list(FM),
                GB = brick_hash:chash_nextfloat_list_to_gb_tree(NF),
                gb_next_always_works(NF, GB)
            end).

prop_2(N_Times) ->
    numtests(N_Times, prop_2()).

gen_weight_list(AllowDuplicatesP) ->
    ?LET({Len, V_MEMBERS},
         {choose(1, 10), if AllowDuplicatesP -> atoms();
                            true             -> [true, false]
                         end},
         ?LET(V, vector(Len, {oneof(V_MEMBERS),choose(1,20)}),
              begin
                  V2 = convert_weight_list(V, 1, []),
                  if AllowDuplicatesP -> % Overload meaning: normalize!
                          Sum = sum_all_weights(V2),
                          [{Atom, Wt/Sum} || {Atom, Wt} <- V2];
                     true ->
                          if V2 /= [] ->
                                  V2;
                             true ->
                                  [{oneof(atoms()), 1}]
                          end
                  end
              end)).

atoms() ->
    [a,b,c,d,e,f,g,h,i,j].

convert_weight_list([{true, Weight}|T], Pos, Acc) ->
    Atom = lists:nth(Pos, atoms()),
    [{Atom, Weight}|convert_weight_list(T, Pos + 1, Acc)];
convert_weight_list([{false, _}|T], Pos, Acc) ->
    convert_weight_list(T, Pos + 1, Acc);
convert_weight_list([H|T], Pos, Acc) ->
    [H|convert_weight_list(T, Pos + 1, Acc)];
convert_weight_list([], _Pos, Acc) ->
    lists:reverse(Acc).

normalize(WL) ->
    Sum = sum_all_weights(WL),
    [{Atom, Wt/Sum} || {Atom, Wt} <- combine_neighbors(lists:sort(WL))].

within_margin(Sum, Margin, Target) ->
    if (Target - Margin) =< Sum, Sum =< (Target + Margin) ->
            true;
       true ->
            %% io:format("Error: Sum = ~20.18f outside of margin ~20.18f target ~20.18f\n", [Sum, Margin, Target]),
            false
    end.

%% Easier to read (?), but cut-and-paste'ish
gen_float_list_alt() ->
    ?LET(Len,
         choose(1, 10),
         ?LET(V, vector(Len, {oneof(atoms()),choose(1,20)}),
              begin
                  V2 = convert_weight_list(V, 1, []),
                  Sum = sum_all_weights(V2),
                  [{Atom, Wt/Sum} || {Atom, Wt} <- V2] % normalize!
              end)).

%% Easier to read (?), but cut-and-paste'ish
gen_weight_list_alt() ->
    ?LET(Len,
         choose(1, 10),
         ?LET(V, vector(Len, {oneof([true, false]),choose(1,20)}),
              begin
                  V2 = convert_weight_list(V, 1, []),
                  if V2 /= [] ->
                          V2;
                     true ->
                          [{oneof(atoms()), 1}]
                  end
              end)).

sum_all_weights(ChainWeights) ->
    lists:foldl(fun({_Ch, Weight}, Sum) -> Sum + Weight end, 0.0, ChainWeights).
combine_neighbors([{Ch, Wt1}, {Ch, Wt2}|T]) ->
    combine_neighbors([{Ch, Wt1 + Wt2}|T]);
combine_neighbors([H|T]) ->
    [H|combine_neighbors(T)];
combine_neighbors([]) ->
    [].

gb_next_always_works([], _GbTree) ->
    true;
gb_next_always_works(NextFloatList, GbTree) ->
    {_Fl, LastChain} = lists:last(NextFloatList),
    SecondForPair = tl(NextFloatList) ++ [{the_end, LastChain}],
    %%
    %% Logic: Use ?MARGIN to pick positions to the "left" and "right" of
    %%        each position in the nextfloat_list.  For the position to the
    %%        left, chash_gb_next should give the current chain.  For the
    %%        positions both exactly here and to the right, chash_bg_next
    %%        should give NextChain.
    %% We assume that ?MARGIN is smaller than the smallest legitimate
    %% difference between two bricks in NextFloatList.
    F = fun({{Pos, Chain}, {NextPos, NextChain}}) ->
                {Pos, Chain} = brick_hash:chash_gb_next(Pos - ?MARGIN, GbTree),
                if NextPos == the_end ->
                        %% smart_exceptions will warn about export on next line
                        {_, LastChain} = brick_hash:chash_gb_next(Pos, GbTree),
                        {_, LastChain} = brick_hash:chash_gb_next(Pos + ?MARGIN, GbTree);
                   true ->
                        {NextPos, NextChain} =
                            brick_hash:chash_gb_next(Pos, GbTree),
                        {NextPos, NextChain} =
                            brick_hash:chash_gb_next(Pos + ?MARGIN, GbTree)
                end,
                true
        end,
    lists:all(F, lists:zip(NextFloatList, SecondForPair)).

-endif. %% -ifdef(QC).
