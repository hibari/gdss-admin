%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2013 Hibari developers.  All rights reserved.
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
%%% File    : repair_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(repair_eqc_tests).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-define(GMTQC, proper).
-undef(EQC).
-endif. %% -ifdef(PROPER).

-ifdef(EQC).
-eqc_group_commands(false).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(GMTQC, eqc).
-undef(PROPER).
-endif. %% -ifdef(EQC).

-ifdef(GMTQC).

-compile(export_all).

run() ->
    run(500).

run(NumTests) ->
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

prop_repair(KeyDataFile, ChainName, HeadBrick, TailBricks) ->
    %% Setup before ?FORALL
    setup_brick(KeyDataFile, HeadBrick),
    NumKeys = length(get_many_witness_ir(HeadBrick, node(), 99999999999)),
    Keys = get_many_witness_ir(HeadBrick, node(), NumKeys+50),
    NumKeys = length(Keys),                     % sanity
    KeysT = list_to_tuple(Keys),
    AllBricks = [HeadBrick|TailBricks],
    MaxRepairWaitTime = 30,                     % seconds

    %% The real work....
    ?FORALL({DelIdxs, InsIdxs, OldIdxs, DelHeadIdxs, DoCheckAndScav},
            {gen_indexes(NumKeys), gen_indexes(NumKeys), gen_indexes(NumKeys),
             gen_indexes(NumKeys), frequency([{2, true}, {99, false}])},
            begin
                if DoCheckAndScav ->
                        gmt_hlog_common:start_scavenger_commonlog([]),
                        [brick_server:checkpoint(Br, node()) ||
                            Br <- AllBricks],
                        timer:sleep(5*1000);
                   true ->
                        ok
                end,
                %% Delete from head of tail bricks, will propagate down.
                delete_keys_ir(hd(TailBricks), node(), DelIdxs, KeysT),
                set_keys_ir(hd(TailBricks), node(), InsIdxs, KeysT),
                make_old_keys_ir(hd(TailBricks), node(), OldIdxs, KeysT),

                {ok, FH1} = file:open("zzz.justincase.history", [append]),
                io:format(FH1, "~w.\n", [{DelIdxs, InsIdxs, OldIdxs}]),
                file:close(FH1),

                ok = sys:suspend(brick_sb),
                KeysAfterHeadDel =
                    try
                        [ok = brick_shepherd:stop_brick(Br, node()) ||
                            Br <- TailBricks],
                        %% OK, now delete stuff from the head.
                        delete_keys_ir(HeadBrick, node(), DelHeadIdxs, KeysT),
                        get_many_witness_ir(HeadBrick, node(), NumKeys+50)
                    after
                        ok = sys:resume(brick_sb)
                    end,
                ok = poll_chain_healthy(AllBricks, node(), ChainName,
                                        MaxRepairWaitTime),
                Rs = [get_many_witness_ir(Br, node(), NumKeys+50)
                      || Br <- AllBricks],
                all_done(),
                AllRs = [KeysAfterHeadDel|Rs],
                [_LAfterDel, L1, L2|XX_rest] = AllRs,
                LEN = lists:usort(AllRs),
                if length(LEN) == 1 ->
                        true;
                   true ->
                        error_logger:warning_msg("ERROR: _LAfterDel == L1 = ~p\n",
                                                 [_LAfterDel == L1]),
                        error_logger:warning_msg("ERROR: L1 == L2 = ~p\n",
                                                 [L1 == L2]),
                        case XX_rest of
                            [] ->
                                ok;
                            [L3|_] ->
                                error_logger:warning_msg("ERROR: L2 == L3 = ~p\n",
                                                         [L2 == L3])
                        end,
                        false
                end
            end).

%% Example usage:
%%
%%     setup_brick("/path/to/key_ts_val_data_file", tab1_ch1_b1).

setup_brick(DataFile, HeadBrick) ->
    setup_brick(DataFile, HeadBrick, node(), false).

setup_brick(DataFile, HeadBrick, Node, VerboseP) ->
    Fverbose = fun(Fmt, Args) -> if VerboseP -> io:format(Fmt, Args);
                                    true     -> ok
                                 end
               end,
    Lold = get_many_witness_ir(HeadBrick, Node, 9999999999),
    Fverbose("Deleting ~p keys from ~p ~p ... ",
             [length(Lold), HeadBrick, Node]),
    _ = delete_ir2(HeadBrick, Node, [Key || {Key, _TS} <- Lold]),
    Fverbose("done\n", []),

    %% Read a list of tuples of at least size 3:
    %% key @ element 1, val @ element 3
    Lnew = try
               {ok, B1} = file:read_file(DataFile),
               binary_to_term(B1)
           catch _:_ ->
                   {ok, L2} = file:consult(DataFile),
                   L2
           end,
    KVs = [{element(1, X), element(3, X)} || X <- Lnew],
    Fverbose("Setting ~p keys from ~p ~p ... ", [length(KVs), HeadBrick, Node]),
    SetRes = set_ir2(HeadBrick, Node, KVs),
    Fverbose("done (all ok = ~p)\n", [lists:usort(SetRes) == [ok]]),
    ok.

gen_indexes(NumKeys) ->
    ?LET(Is,
         frequency([
                    {10, 0}
                    , {10, ?LET(X,       nat(),               X+1)}
                    , {20, ?LET({X,Y},   {nat(),nat()},       (X+1) * (Y+1))}
                    , {10, ?LET({X,Y,Z}, {nat(),nat(),nat()}, (X+1) * (Y+1) * (Z+1))}
                    , {50, gen_ranges(NumKeys)}
                   ]),
         if is_integer(Is) ->
                 lists:duplicate(Is, choose(1, NumKeys));
            is_list(Is) ->
                 Is
         end).

gen_ranges(NumKeys) ->
    ?LET({Where, I, FromEnd, Start},
         {choose(0,7), nat(), nat(), choose(1, NumKeys)},
         lists:append(
           [lists:seq(1, erlang:min(NumKeys, I+1)) ||
               Where band 1 /= 0] ++
               [lists:seq(Start+1, erlang:min(NumKeys, Start + I)) ||
                   Where band 2 /= 0] ++
               [lists:seq(NumKeys - FromEnd, erlang:min(NumKeys, NumKeys - FromEnd + I)) ||
                   Where band 4 /= 0])
        ).

get_many_witness_ir(Br, Nd, MaxNum) ->
    get_many_ir(Br, Nd, MaxNum, [witness]).

get_many_ir(Br, Nd, MaxNum, Flags) ->
    Op = brick_server:make_get_many(<<>>, MaxNum, Flags),
    [{ok, {L, _Bool}}] = brick_server:do(Br, Nd, [Op], [ignore_role], 60*1000),
    L.

delete_ir(Br, Nd, Key) ->
    [Res] = delete_ir2(Br, Nd, [Key]),
    Res.

delete_ir2(Br, Nd, Keys) ->
    Ops = [brick_server:make_delete(Key) || Key <- Keys],
    brick_server:do(Br, Nd, Ops, [ignore_role], 60*1000).

set_ir(Br, Nd, Key, Val) ->
    [Res] = set_ir2(Br, Nd, [{Key, Val}]),
    Res.

set_ir2(Br, Nd, KVs) ->
    Ops = [brick_server:make_set(Key, Val) || {Key, Val} <- KVs],
    brick_server:do(Br, Nd, Ops, [ignore_role], 60*1000).

make_old_ir(Br, Nd, Key, Val) ->
    [Res] = make_old_ir2(Br, Nd, [{Key, Val}]),
    Res.

make_old_ir2(Br, Nd, KVs) ->
    _XX1 = delete_ir2(Br, Nd, [Key || {Key, _} <- KVs]),
    Ops = [brick_server:make_op6(set, Key, 4242, Val, 0, []) ||
              {Key, Val} <- KVs],
    brick_server:do(Br, Nd, Ops, [ignore_role], 60*1000).

delete_keys_ir(Br, Nd, DelIdx, KeysT) ->
    Keys = [begin {Key, _TS} = element(Idx, KeysT), Key end || Idx <- DelIdx],
    delete_ir2(Br, Nd, Keys).

set_keys_ir(Br, Nd, DelIdx, KeysT) ->
    KVs = [begin {Key, _TS} = element(Idx, KeysT),
                 {Key, "bar set replacement val"}
           end || Idx <- DelIdx],
    set_ir2(Br, Nd, KVs).

make_old_keys_ir(Br, Nd, DelIdx, KeysT) ->
    KVs = [begin {Key, _TS} = element(Idx, KeysT),
                 {Key, "bar set replacement val"}
           end || Idx <- DelIdx],
    make_old_ir2(Br, Nd, KVs).

poll_chain_healthy(Bricks, Node, ChainName, MaxRepairWaitTime) ->
    PollInterval = 50,                          % milliseconds
    PollIters = (MaxRepairWaitTime * 1024) div PollInterval,
    [begin
         ok = poll_repair_state({Br, Node}, ok, PollInterval, PollIters),
         %%error_logger:warning_msg("\n\n\n brick ~p is ok\n\n\n", [Br]),
         %%timer:sleep(1000)
         ok
     end || Br <- Bricks],
    ok = poll_chain_healthy_status(ChainName, PollInterval, PollIters),
    error_logger:info_msg("~s: chain ~p healthy\n", [?MODULE, ChainName]),

    %% Because we were sending set/delete updates to bricks in the
    %% middle/end of the chain, we violated a design property
    %% regarding locally-maintained log serial numbers.  We need to
    %% wait for the Admin Server's log flush to make certain all
    %% bricks fall back into agreement on log serial numbers.
    ok = poll_chain_serials_equal(Bricks, PollInterval, PollIters),
    %% Oh !W#$!@, that isn't sufficient either, because ack'ed serial
    %% numbers are reset when roles are changed, and roles can change
    %% after poll_chain_serials_equal() above has told is all is OK.
    timer:sleep(2000),
    ok.

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

poll_chain_healthy_status(ChainName, SleepMs, MaxIters) ->
    RepairPoll =
        fun(0) ->
                {false, failed};
           (Acc) ->
                case (catch brick_sb:get_status(chain, ChainName)) of
                    {ok, healthy} ->
                        {false, ok};
                    _St ->
                        timer:sleep(SleepMs),
                        {true, Acc - 1}
                end
        end,
    gmt_loop:do_while(RepairPoll, MaxIters).

poll_chain_serials_equal([HeadBrick|_], SleepMs, MaxIters) ->
    SerialPoll =
        fun(0) ->
                {false, failed};
           (Acc) ->
                DownSerial = get_chain_prop(HeadBrick, chain_down_serial),
                AckSerial = get_chain_prop(HeadBrick, chain_down_acked),
                if DownSerial == AckSerial ->
                        error_logger:info_msg("~s: acks OK\n", [?MODULE]),
                        {false, ok};
                   true ->
                        timer:sleep(SleepMs),
                        {true, Acc - 1}
                end
        end,
    gmt_loop:do_while(SerialPoll, MaxIters).

get_chain_prop(Brick, PropName) ->
    get_chain_prop(Brick, node(), PropName).

get_chain_prop(Brick, Node, PropName) ->
    Cs = proplists:get_value(chain,
                             element(2,brick_server:status(Brick, Node))),
    proplists:get_value(PropName, Cs).

min(X, Y) when X < Y -> X;
min(_, Y)            -> Y.

all_done() ->
    ok.

-endif. %% -ifdef(GMTQC).
