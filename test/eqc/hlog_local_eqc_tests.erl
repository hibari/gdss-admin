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
%%% File    : hlog_local_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(hlog_local_eqc_tests).

-ifdef(QC).

-include_lib("qc/include/qc.hrl").

-include("gmt_hlog.hrl").
-include_lib("kernel/include/file.hrl").

-compile(export_all).

%% eqc_statem callbacks
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-define(MUT, gmt_hlog_local).

-define(MAX_LOCALS, 3). % No more than 10, or adjust the macros below.
-define(NAMES_LOCALS,
        {yyyl_qc_1, yyyl_qc_2, yyyl_qc_3, yyyl_qc_4, yyyl_qc_5,
         yyyl_qc_6, yyyl_qc_7, yyyl_qc_8, yyyl_qc_9, yyyl_qc_10}).
-define(NAMES_REFERENCES,
        {yyyr_qc_1, yyyr_qc_2, yyyr_qc_3, yyyr_qc_4, yyyr_qc_5,
         yyyr_qc_6, yyyr_qc_7, yyyr_qc_8, yyyr_qc_9, yyyr_qc_10}).

-record(state, {
          step = 1,
          num_logs = 0,                          % integer
          hunks_bigblob = []
         }).

run() ->
    run(500).

run(NumTests) ->
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

prop_local_log() ->
    io:format("\n\nNOTE: GDSS app can't be running while this test runs.\n"),
    io:format("      Run: application:stop(gdss).\n\n"),
    %%timer:sleep(2000),
    ?FORALL(Cmds,
            more_commands(5,commands(?MODULE)),
            collect({length(Cmds) div 10, div10},
                    begin
                        stop_all(),
                        delete_all(),
                        {_Hist, S, Res} = run_commands(?MODULE, Cmds),
                        if S#state.num_logs > 0 -> sync_full_writeback();
                           true                 -> ok
                        end,
                        %% NOTE: Hack here
                        catch gmt_hlog:stop(commonLogServer),
                        %% stop_all(),
                        ?WHENFAIL(begin
                                      io:format("S = ~p\nR = ~p\n", [S, Res]),
                                      %%io:format("H = ~p\n", [_Hist]),
                                      ok
                                  end,
                                                %Res == ok andalso locals_match_references(S))
                                  Res == ok andalso all_bigblobs_ok(S))
                    end)).

%% command :: (S::symbolic_state()) :: gen(call() | stop)
%% Called in symbolic context.

command(S) when S#state.step == 1 ->
    {call, ?MODULE, start_log_procs, [choose(1, ?MAX_LOCALS)]};
command(S) ->
    frequency(
      [{200, {call, ?MODULE, write_hunk,
              [choose(1, S#state.num_logs), gen_logtype(), nat(),
               gen_blobs(num_blobs()), gen_blobs(num_blobs())]}},
       {200, {call, ?MODULE, write_read_hunk,
              [choose(1, S#state.num_logs), gen_logtype(), nat(),
               gen_blobs(num_blobs()), gen_blobs(num_blobs())]}},
       {30, {call, ?MODULE, async_full_writeback, [choose(1,100)]}},
       {30,  {call, ?MODULE, advance_seqnum, [{var,1}, int()]}},
       {30,  {call, ?MODULE, async_advance_seqnum, [{var,1}, int(), nat()]}},
       {30,  {call, ?MODULE, async_sync, [{var,1}, nat()]}}
       %% no?, {2, {call, ?MODULE, sync_full_writeback, []}}
       %% No?? , {10,  {call, ?MODULE, advance_seqnum, [{var,1}, int()]}}
      ]).

gen_logtype() ->
    %%oneof([metadata]).
    %%oneof([metadata, bigblob]).
    oneof([bigblob]).

num_blobs() ->
    %% Don't make too many blobs, or else we're hit gmt_hlog's limit
    %% ?MAX_HUNK_HEADER_BYTES.
    choose(0, 3).

%% initial_state() :: symbolic_state().
%% Called in symbolic context.

initial_state() ->
    %%     stop_all(),
    %%     delete_all(),
    #state{}.

%% precondition(S::symbolic_state(), C::call()) :: bool()
%% Called in symbolic (??) & runtime context.

precondition(_S, {call, _, start_log_procs, _}) ->
    true;
precondition(S, {call, _, write_hunk, _}) ->
    S#state.num_logs > 0;
precondition(S, {call, _, write_read_hunk, _}) ->
    S#state.num_logs > 0;
precondition(S, {call, _, async_full_writeback, _}) ->
    S#state.num_logs > 0;
precondition(S, {call, _, sync_full_writeback, _}) ->
    S#state.num_logs > 0;
precondition(_S, {call, _, advance_seqnum, [_, Incr]}) ->
    Incr /= 0;
precondition(_S, {call, _, async_advance_seqnum, [_, Incr, _]}) ->
    Incr /= 0;
precondition(S, {call, _, async_sync, _}) ->
    S#state.num_logs > 0;
precondition(_S, Call) ->
    io:format("\nprecondition: what the heck is this?  ~p\n", [Call]),
    timer:sleep(500),
    false.

%% postcondition(S::dynamic_state(),C::call(),R::term()) :: bool()
%% Called in runtime context.

postcondition(S, C, R) ->
    case postcondition2(S, C, R) of
        true ->
            true;
        false ->
            io:format("BUMMER: ~p at ~p\n", [R, C]),
            false
    end.

postcondition2(_S, {call, _, start_log_procs, _}, _R) ->
    true;
postcondition2(_S, {call, _, write_hunk, _}, {ok, _, _} = _R) ->
    %%io:format(",R=~p,", [_R]),
    true;
postcondition2(_S, {call, _, write_hunk, _}, _R) ->
    false;
postcondition2(_S, {call, _, write_read_hunk, _}, {ok, _} = _R) ->
    %%io:format(",R=~p,", [_R]),
    true;
postcondition2(_S, {call, _, write_read_hunk, _}, _R) ->
    false;
postcondition2(_S, {call, _, async_full_writeback, _}, _R) ->
    true;
postcondition2(_S, {call, _, sync_full_writeback, _}, R) ->
    R == ok;
postcondition2(_S, {call, _, advance_seqnum, _}, R) ->
    case R of
        {ok, _} -> true;
        error   -> false
    end;
postcondition2(_S, {call, _, async_advance_seqnum, _}, _R) ->
    true;
postcondition2(_S, {call, _, async_sync, _}, _R) ->
    true;
postcondition2(_S, _Call, _R) ->
    io:format("\npostcondition: what the heck is this?  ~p\n", [_Call]),
    timer:sleep(1000),
    false.

%% next_state(S::symbolic_state(),R::var(),C::call()) :: symbolic_state()
%% Called in symbolic & runtime context.

next_state(S, _Res, {call, _, start_log_procs, [Num]}) ->
    S#state{step = S#state.step + 1, num_logs = Num};
next_state(S, {ok, SeqNum, Offset}, {call, _, write_hunk,
                                     [_Num, bigblob, TypeNum, CBs, Bs]}) ->
    H = {SeqNum, Offset, TypeNum, CBs, Bs},
    Hunks = S#state.hunks_bigblob,
    S#state{hunks_bigblob = Hunks ++ [H]};
next_state(S, {ok, NewHunks}, {call, _, write_read_hunk,
                               [_Num, bigblob, _TypeNum, _CBs, _Bs]}) ->
    Hunks = S#state.hunks_bigblob,
    S#state{hunks_bigblob = Hunks ++ NewHunks};
next_state(S, _Res, {call, _, write_hunk, _}) ->
    S;
next_state(S, _Res, {call, _, write_read_hunk, _}) ->
    S;
next_state(S, _Res, {call, _, async_full_writeback, _}) ->
    S;
next_state(S, _Res, {call, _, sync_full_writeback, _}) ->
    S;
next_state(S, _Res, {call, _, advance_seqnum, _}) ->
    S;
next_state(S, _Res, {call, _, async_advance_seqnum, _}) ->
    S;
next_state(S, _Res, {call, _, async_sync, _}) ->
    S.

gen_blobs(0) ->
    [];
gen_blobs(Num) ->
    noshrink(?LET(I, Num, vector(I, gen_blob()))).

gen_blob() ->
    ?LET({I, C}, {nat(), choose(0,255)},
         list_to_binary(my_duplicate(I, C))).

my_duplicate(0, _) ->
    [];
my_duplicate(N, X) ->
    lists:duplicate(N, X).

%%%%
%%%%

write_hunk(Num, LogType, TypeNum, CBs, Bs) ->
    %%io:format("W ~p ~p ", [length(CBs), length(Bs)]),
    io:format("w"),
    RefName = element(Num, ?NAMES_REFERENCES),
    RefReg = n2regn(RefName),
    {ok, _, _} = ?MUT:write_hunk(RefReg, RefName,   LogType, <<"k:">>,
                                 TypeNum, CBs, Bs),
    LocalName = element(Num, ?NAMES_LOCALS),
    LocalReg = n2regn(LocalName),
    ?MUT:write_hunk(           LocalReg, LocalName, LogType, <<"k:">>,
                               TypeNum, CBs, Bs).

big_bin(N) ->
    term_to_binary(lists:seq(1,N)).

write_read_hunk(Num, LogType, TypeNum, CBs, Bs) ->
    %% write some hunks and read summary of them
    NumHunks = 10,
    %%io:format("W ~p ~p ", [length(CBs), length(Bs)]),
    io:format("R"),
    RefName = element(Num, ?NAMES_REFERENCES),
    RefReg = n2regn(RefName),
    {ok, _, _} = ?MUT:write_hunk(RefReg, RefName,
                                 LogType, <<"k:">>,
                                 TypeNum, CBs, Bs),
    LocalName = element(Num, ?NAMES_LOCALS),
    LocalReg = n2regn(LocalName),

    {SeqOffL, Hunks} =
        lists:foldl(
          fun(_X,{A,B}) ->
                  {ok, SeqNum, Offset} = ?MUT:write_hunk(
                                            LocalReg, LocalName, LogType,
                                            big_bin(10*1000),
                                            TypeNum, CBs, Bs),
                  Hnk = {SeqNum, Offset, TypeNum, CBs, Bs},
                  {[{SeqNum,Offset}|A], [Hnk|B]}
          end, {[],[]}, lists:seq(1,NumHunks)),

    Result = [catch gmt_hlog:read_hunk_summary(
                      "hlog.commonLogServer", Seq, Off) ||
                 {Seq,Off} <- SeqOffL],
    lists:foreach(fun (T) when is_tuple(T) ->
                          if element(1,T)==hunk_summ ->
                                  io:format("r");
                             true ->
                                  io:format("~n:::~p:::~n",[{summary_error,T}])
                          end;
                      (Err) ->
                          io:format(":~p:",[Err])
                  end, Result),
    {ok, lists:reverse(Hunks)}.


async_full_writeback(SleepTime) ->
    io:format("a"),
    spawn(fun() -> timer:sleep(SleepTime), sync_full_writeback() end).

sync_full_writeback() ->
    %%io:format(" writeback "),
    gmt_hlog_common:full_writeback(?GMT_HLOG_COMMON_LOG_NAME).

advance_seqnum(_Server, Incr) ->
    %% NOTE: hack here
    io:format("A"),
    ?MUT:advance_seqnum(commonLogServer_hlog, Incr).

async_advance_seqnum(_Server, Incr, SleepTime) ->
    io:format("<"),
    spawn(fun() -> timer:sleep(SleepTime),
                   io:format("<"),
                   %% NOTE: hack here
                   ?MUT:advance_seqnum(commonLogServer_hlog, Incr)
          end).

async_sync(_Server, SleepTime) ->
    io:format("("),
    spawn(fun() -> timer:sleep(SleepTime),
                   %% NOTE: hack here
                   io:format(")"),
                   ?MUT:sync(commonLogServer_hlog)
          end).

%%%%
%%%%

stop_all() ->
    catch gmt_hlog_common:stop(?GMT_HLOG_COMMON_LOG_NAME),
    [catch ?MUT:stop(n2regn(L)) || L <- tuple_to_list(?NAMES_LOCALS)],
    [catch gmt_hlog:stop(n2regn(L)) || L <- tuple_to_list(?NAMES_REFERENCES)],
    ok.

delete_all() ->
    os:cmd("rm -rf " ++ ?MUT:log_name2data_dir(?GMT_HLOG_COMMON_LOG_NAME)
           ++
               [[" ", ?MUT:log_name2data_dir(L), " "] ||
                   L <- tuple_to_list(?NAMES_LOCALS)]
           ++
               [[" ", ?MUT:log_name2data_dir(L), " "] ||
                   L <- tuple_to_list(?NAMES_REFERENCES)]
          ),
    ok.

start_log_procs(Num) ->
    case gmt_hlog_common:start([{common_log_name, ?GMT_HLOG_COMMON_LOG_NAME},
                                suppress_scavenger]) of
        {ok, _} -> ok;
        ignore  -> ok
    end,
    lists:foreach(
      fun(X) ->
              {ok, _} = ?MUT:start_link([{name, element(X, ?NAMES_LOCALS)}]),
              {ok, _} = gmt_hlog:start_link([{name, element(X, ?NAMES_REFERENCES)}])
      end, lists:seq(1, Num)).

n2regn(Name) ->
    gmt_hlog:log_name2reg_name(Name).

locals_match_references(S) ->
    lists:all(fun(N) ->
                      %% Remove all reference .HLOG files that are
                      %% exactly 12 bytes: they were never written to.
                      %% Over in the local log dir, that file won't
                      %% exist, so deleting the reference file will
                      %% keep "diff" from complaining.  ... Yes, if
                      %% commented out, you'll get the counter-ex.
                      os:cmd("find hlog.yyyr_qc_" ++ integer_to_list(N)
                             ++ " -size 12c -name '*.HLOG' | xargs rm"),
                      X = os:cmd("diff -r hlog.yyy?_qc_" ++ integer_to_list(N)
                                 ++ " | egrep -v 'Only in .*: [123]$|\\.TMP$|\\.TRK$|Config|sequence_number'"),
                      X == ""
              end, lists:seq(1, S#state.num_logs)).

all_bigblobs_ok(S) ->
    case get(moofoo) of
        undefined ->
            io:format("\n\nNOTE: we are only checking for all readable bigblob hunk summaries\n\n"),
            put(moofoo, true);
        _ ->
            ok
    end,
    {Cheat, []} = hlog_eqc_tests:wal_fold(shortterm, "hlog.commonLogServer"),
    %%     io:format("Cheat: ~w\n", [Cheat]),
    %%     io:format("State: ~w\n", [S#state.hunks_bigblob]),
    Cheat = S#state.hunks_bigblob,
    lists:foldl(fun(_X, false) ->
                        false;
                   ({SeqNum, Offset, _, _, _}, true) ->
                        T = gmt_hlog:read_hunk_summary(
                              "hlog.commonLogServer", SeqNum, Offset),
                        element(1, T) == hunk_summ
                end, true, S#state.hunks_bigblob).

nul_check() ->
    Files = string:tokens(os:cmd("find . -name '*.HLOG' | grep -v hlog.commonLogServer | sort"), "\n"),
    [find_nul_blocks_in(F) || F <- Files].

find_nul_blocks_in(File) ->
    {ok, B} = file:read_file(File),
    B32 = list_to_binary(lists:duplicate(32, 0)),
    find_nul_block(B, 0, B32, File).

find_nul_block(B, Offset, Block, File) when Offset < size(B) ->
    BS = size(Block),
    case B of
        <<_:Offset/binary, Block:BS/binary, _/binary>> ->
            io:format("File ~s, NULs start at offset ~p\n", [File, Offset]),
            find_first_nonnul(B, Offset);
        _ ->
            find_nul_block(B, Offset+1, Block, File)
    end;
find_nul_block(_, _, _, _) ->
    ok.

find_first_nonnul(B, Offset) when Offset < size(B) ->
    case B of
        <<_:Offset/binary, C:8, _/binary>> when C /= 0 ->
            io:format("First nonNUL byte at offset ~p\n", [Offset]);
        _ ->
            find_first_nonnul(B, Offset+1)
    end;
find_first_nonnul(_, _) ->
    ok.

-endif. %% -ifdef(QC).
