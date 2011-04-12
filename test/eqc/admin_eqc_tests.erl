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
%%% File    : admin_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(admin_eqc_tests).

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

-define(TABLE, tab1).
-define(INIT_IDLING, [newb0, newb1, newb2, newb3, newb4, newb5]).
-define(RINTERVAL, 1000).
%%-define(DEBUG_QC, true).

-compile(export_all).

%% eqc_statem callbacks
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

%% note that this abstract state is not perfectly synchronized with the real chians
%% of bricks because change_chain_length is not synchronous
-record(state, {
          last_result,
          chains,      % [{ChainName, Bricks}]
          starting_chains,
          idling_bricks % [Brick]
         }).

run() ->
    run(500).

run(NumTests) ->
    brick_eunit_utils:setup_and_bootstrap(),
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

%% props %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_0() ->
    common1_prop(fun(X, _S) ->
                         %%dbglog("~nSSS: ~p~n", [S#state.chains]),
                         X==ok
                 end,
                 []).

common1_prop(F_check, Env) ->
    ?FORALL(Cmds, commands(?MODULE),
            collect(length(Cmds),
                    begin
                        dbglog("======now running ~p commands=========~n",[length(Cmds)]),
                        {_Hist, S, Res} = run_commands(?MODULE, Cmds, Env),
                        cleanup(S),
                        ?WHENFAIL(begin
                                      dbglog("S = ~p\nR = ~p\n", [S, Res]),
                                      ok
                                  end,
                                  F_check(Res, S))
                    end)).

cleanup(S) ->
    dbglog("~n---cleanup starting---~n"),
    sync_chains(S),
    MaxRetry = 16,
    [ok = sync_change_chain_length_safe(CN, NB,?RINTERVAL,MaxRetry) ||
        {CN, NB} <- S#state.starting_chains],
    dbglog("~n---cleanup finished---~n").

%% init %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
initial_state()->
    {ok, Chains} = brick_admin:get_table_chain_list(?TABLE),
    %%dbglog("~nINIT::: ~p~n", [Chains]),
    Idling =   [{B,node()} || B <- ?INIT_IDLING],
    %%dbglog("INIT::: starting with ~p~n~p~n:::INIT~n", [Chains,Idling]),
    #state{chains=Chains, last_result=init, starting_chains=Chains,
           idling_bricks=Idling}.

%% commands %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
command(S) ->
    frequency(
      [{50,  {call, ?MODULE, brick_add, [new_brick(S), chain_name(S), S]}},
       {50,  {call, ?MODULE, brick_delete, [brick(S), chain_name(S), S]}}
      ]).


brick_add(Brick={Bname,_Node}, ChainName, S) ->
    dbglog("~n---sync before adding ~p to ~p---~n", [Bname, ChainName]),
    Chains = sync_chains(S),
    {ChainName, OldBricks} = find_chain(ChainName, Chains),
    NewBricks = [Brick | OldBricks],
    dbglog("~n---brick_add starting---"),
    Ret = brick_admin:change_chain_length(ChainName, NewBricks),
    dbglog("finished---~p~n", [Ret]),
    Ret.


brick_delete(Brick={Bname,_Node}, ChainName, S) ->
    dbglog("~n---sync before deleting ~p from ~p---~n", [Bname, ChainName]),
    Chains = sync_chains(S),
    {ChainName, OldBricks} = find_chain(ChainName, Chains),
    NewBricks = lists:delete(Brick, OldBricks),
    %%dbglog("Cmd:::DEL(~p)::::::::::~nOLD:: ~p~nNEW:: ~p~n", [ChainName,OldBricks, NewBricks]),
    if length(NewBricks) > 0 ->
            dbglog("~n---brick_delete starting---"),
            Ret = brick_admin:change_chain_length(ChainName, NewBricks),
            dbglog("finished---~p~n", [Ret]),
            Ret;
       true ->
            dbglog("0"),
            ok %% do not delete if this is the last brick
    end.



%% generators %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
new_brick(S)->
    oneof([nil|S#state.idling_bricks]).

brick(S) ->
    oneof(all_bricks(S#state.chains)).

chain_name(S) ->
    oneof([CN || {CN, _} <- S#state.chains]).


%% preconditons %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
precondition(_S, {call, _, brick_add, [Brick, _ChainName, S]}) ->
    BS = all_bricks(S#state.chains),
    not(lists:member(Brick, BS)) andalso Brick /= nil;
precondition(_S, {call, _, brick_delete, [Brick, ChainName, S]}) ->
    {ChainName, BS} = find_chain(ChainName, S#state.chains),
    lists:member(Brick, BS) andalso length(BS) > 1.

%% postcoditions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
postcondition(_S, {call, _, C, [Brick, ChainName, _]}, R) ->
    case R of
        ok ->
            true;
        %%      {'EXIT', {_, _, {error, duplicate_bricks_in_chain}}} ->
        %%          true;
        %%      {'EXIT', {_, _, {error,current_chains_too_short}}} ->
        %%          true;
        %%      {'EXIT', {_, _, {error, same_list}}} ->
        %%          true;
        %%      {'EXIT', {_, _, {error, no_intersection}}} ->
        %%          true;
        %%      {'EXIT', {_, _, match,
        %%                 [{error,
        %%                   {_, _, {error,duplicate_brick_names}}}]}} ->
        %%          true;
        %%      {'EXIT', {_, _, match,
        %%                 [{error,
        %%                   {_, _, {error,duplicate_bricks_in_chain}}}]}} ->
        %%          true;
        _ ->
            {C,R,Brick,ChainName}
    end.

%% state transitions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
next_state(S, R, {call, _, C, [Brick, ChainName, _S]}) ->
    {NewChains, NewIdling} =
        case C of
            brick_add ->
                {add_brick(Brick, ChainName, S#state.chains),
                 lists:delete(Brick, S#state.idling_bricks)};
            brick_delete ->
                {delete_brick(Brick, ChainName, S#state.chains),
                 [Brick|S#state.idling_bricks]}
        end,
    S#state{chains=NewChains, idling_bricks=NewIdling,
            last_result={C,R,Brick,ChainName}}.





%% misc %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
add_brick(Brick, ChainName, [{ChainName, Bricks}|CDR]) ->
    [{ChainName, [Brick|Bricks]}|CDR];
add_brick(Brick, ChainName, [CAR|CDR]) ->
    [CAR|add_brick(Brick, ChainName, CDR)].

delete_brick(Brick, ChainName, [{ChainName, Bricks}|CDR]) ->
    NewBricks =  lists:filter(fun(B)->B=/=Brick end, Bricks),
    [{ChainName, NewBricks} | CDR];
delete_brick(Brick, ChainName, [CAR|CDR]) ->
    [CAR|delete_brick(Brick, ChainName, CDR)].


find_chain(ChainName, Chains) ->
    [Match] = [{CN, BS} || {CN, BS} <- Chains, CN==ChainName],
    Match.

all_bricks(Chains) ->
    [B || {_, BS} <- Chains, B <- BS].


intersection(L0, L1) ->
    lists:foldl(fun(X,Acc)->
                        case lists:member(X, L1) of
                            true ->
                                [X|Acc];
                            false ->
                                Acc
                        end
                end,
                [], L0).


sync_change_chain_length_safe(CN, NB,RetryInterval,MaxRetry) ->
    dbglog("~n---start deleting busy bricks for ~p---", [CN]),
    {ok, Chains} = brick_admin:get_table_chain_list(?TABLE),
    OtherChains = lists:keydelete(CN, 1, Chains),
    [ok = freebrick(B, OtherChains, RetryInterval, MaxRetry) || B <- NB],
    dbglog("finished---~n"),

    %% if there is no intersection, keep one from the old chain, and delete it later
    {_,OldBricks} = find_chain(CN, Chains),
    OldOne = hd(OldBricks),
    II = intersection(OldBricks, NB),
    case II of
        [] ->
            dbglog("i"),
            sync_change_chain_length(CN, [OldOne|NB],RetryInterval,MaxRetry);
        _ ->
            ok
    end,

    Ret = sync_change_chain_length(CN, NB,RetryInterval,MaxRetry),

    case II of
        [] ->
            %% sync for OldOne
            dbglog("I"),
            until(fun()->ready(OldOne)end, ?RINTERVAL, 7),
            Ret;
        _ ->
            Ret
    end.


freebrick(_B, [], _RetryInterval, _MaxRetry) ->
    ok;
freebrick(B, [{CN,BS} | OtherChains], RetryInterval, MaxRetry) ->
    case lists:member(B, BS) of
        true ->
            NewBS = if
                        [B]==BS -> %% B is the last one
                            dbglog("L"),
                            [list_to_atom(atom_to_list(CN)++"temp") | BS];
                        true ->
                            BS
                    end,
            dbglog("~n---start removing ~p from ~p~n", [B, NewBS]),
            Ret = sync_change_chain_length(CN, lists:delete(B, NewBS), RetryInterval, MaxRetry),
            until(fun()->ready(B)end, ?RINTERVAL, 7),
            dbglog("~n---finished removing ~p from ~p~n", [B, NewBS]),
            Ret;
        false ->
            freebrick(B, OtherChains, RetryInterval, MaxRetry)
    end.

sync_change_chain_length(CN, NB,RetryInterval,MaxRetry) ->
    {ok, OldChains} = brick_admin:get_table_chain_list(?TABLE),
    {CN, OldBricks} = find_chain(CN, OldChains),
    DeletingBricks = OldBricks -- NB,

    case (catch brick_admin:change_chain_length(CN, NB)) of
        ok -> ok;
        {'EXIT', {error, same_list}} ->
            ok;
        {'EXIT', {_, _, {error, same_list}}} ->
            ok
    end,

    F = fun() ->
                {ok, Chains} = brick_admin:get_table_chain_list(?TABLE),
                {CN, BS} = find_chain(CN, Chains),
                if BS==NB -> true;
                   true -> %%dbglog("---~p---~n---~p---~n", [BS, NB]),
                        false
                end
        end,

    %% sync chains
    ok = until(F, RetryInterval, MaxRetry),
    %% wait until deleted bricks get ready
    [ok = until(fun()->ready(B)end, ?RINTERVAL, 7) || B <- DeletingBricks],
    ok.


until(_, _, 0) ->
    timeout_max_retry;
until(F, Interval, Max) ->
    P = F(),
    if P ->
            dbglog("o"), %% success
            ok;
       true ->
            dbglog("~.16B", [Max - 1]), %% retry
            timer:sleep(Interval),
            until(F, Interval, Max-1)
    end.

chains_eq(CS0, CS1) ->
    case length(CS0)==length(CS1) of
        false ->
            dbglog("~n~p /= ~p~n", [length(CS0), length(CS1)]),
            false;
        true ->
            Pred = fun({X,Y}) ->
                           chain_eq(X,Y)
                   end,
            Z = lists:zip(lists:sort(CS0), lists:sort(CS1)),
            length(CS0) == length(lists:takewhile(Pred, Z))
    end.



chain_eq({CN0, BS0}, {CN1, BS1}) ->
    if
        CN0==CN1 ->
            if BS0 /= BS1 ->
                    %%dbglog("~nBS0::~p~nBS1::~p~n", [BS0,BS1]),
                    false;
               true ->
                    true
            end,
            BS0==BS1;
        true ->
            dbglog("~n~p /= ~p~n", [CN0, CN1]),
            false
    end.

%% brick is ready %%
ready(Br={Bname,_}) ->
    case catch brick_server:status(Br, node()) of
        {ok, _} ->
            dbglog("\---~p is still running---~n",[Br]),
            false;
        {'EXIT',{_, _}} ->
            %%dbglog("[~p]",[Bname]),
            true;
        Error ->
            exit({brick_ready_error, Bname, Error})
    end.

%% sync chains and state
sync_chains(S) ->
    F = fun() ->
                {ok, Chains} = brick_admin:get_table_chain_list(?TABLE),
                ChainsEQ = chains_eq(Chains, S#state.chains),
                ChainsEQ andalso
                    length(S#state.idling_bricks) ==
                    length(lists:takewhile(fun ready/1, S#state.idling_bricks))
        end,
    ok = until(F, ?RINTERVAL, 10),
    S#state.chains.

-ifndef(DEBUG_QC).
dbglog(F) ->
    dbglog(F,[]).
dbglog(_F,_L) ->
    ok.
-else.
dbglog(F) ->
    dbglog(F,[]).
dbglog(F,L) ->
    io:format(F,L).
-endif.


%% scratch %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

deladd() ->
    S = initial_state(),
    CN = tab1_ch1,
    Old = [{tab1_ch1_b1,gdss_dev@hotate}, {tab1_ch1_b2,gdss_dev@hotate}],
    NB = [{tab1_ch1_b1,gdss_dev@hotate}],
    B = {tab1_ch1_b2,gdss_dev@hotate},
    io:format("init:: ~p~n", [S]),
    io:format("DEL:: ~p~n",[brick_admin:change_chain_length(CN, NB)]),
    io:format("---:: ~p~n", [catch brick_server:status(B, node()) ]),
    io:format("ADD:: ~p~n",[brick_admin:change_chain_length(CN, Old)]),
    io:format("---:: ~p~n", [catch brick_server:status(B, node()) ]).

-endif. %% -ifdef(GMTQC).
