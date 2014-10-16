%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2014 Hibari developers.  All rights reserved.
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
%%% File    : hlog_qc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(hlog_eqc_tests).

-ifdef(QC).

-include_lib("qc/include/qc.hrl").

-include("gmt_hlog.hrl").
-include_lib("kernel/include/file.hrl").

-compile(export_all).

%% eqc_statem callbacks
-export([initial_state/0, command/1, precondition/2, postcondition/3,
         next_state/3]).

-define(MUT, gmt_hlog).
-define(TEST_NAME, 'zzz-hlog-qc').
-define(TEST_DIR, "hlog.zzz-hlog-qc").

-record(state, {
          step = 1,
          server,                               % pid()
          log_size_kb,                          % integer()
          hunks_short = [],                     % list()
          hunks_long = []                       % list()
         }).

run() ->
    run(500).

run(NumTests) ->
    gmt_eqc:module({numtests,NumTests}, ?MODULE).

prop_log() ->
    prop_log(false).

prop_log(EnableScribbleTestP) ->
    io:format("\nNOTE: Failure due to 'hunk_header_too_big' exception\n"
              "is not a real failure and can be ignored safely.\n\n"),
    %%timer:sleep(1000),
    ?FORALL(Cmds,
            with_parameters([{scribble, EnableScribbleTestP}],
                            commands(?MODULE)),
            collect({length(Cmds) div 10, div10},
                    begin
                        {_Hist, S, Res} = run_commands(?MODULE, Cmds),
                        catch ?MUT:stop(S#state.server),
                        ?WHENFAIL(begin
                                      io:format("S = ~p\nR = ~p\n", [S, Res]),
                                      ok
                                  end,
                                  Res == ok andalso log_matches_state(S) andalso
                                  scribbles_cause_failure(S, Cmds, EnableScribbleTestP))
                    end)).

%% command :: (S::symbolic_state()) :: gen(call() | stop)
%% Called in symbolic context.

command(S) when S#state.step == 1 ->
    {call, ?MODULE, start_server, [choose(2, 10),
                                   small_sleep(), medium_sleep()]};
command(_S) ->
    ?LET(ScribbleP, parameter(scribble),
         frequency(
           [{200, {call, ?MODULE, write_hunk,
                   [{var,1}, shortlong(),
                    nat(), gen_blobs(num_blobs()), gen_blobs(num_blobs())]}},
            {30,  {call, ?MODULE, sleep, [choose(1,50)]}},
            {30,  {call, ?MODULE, advance_seqnum, [{var,1}, int()]}},
            {30,  {call, ?MODULE, async_advance_seqnum, [{var,1}, int(), nat()]}},
            {30,  {call, ?MODULE, async_sync, [{var,1}, nat()]}}
           ] ++
               %%
               %% TODO: There are a couple of things to add to this, either by
               %%       abusing scribble()'s three args or adding new cmds.
               %%       1. Truncate the file.
               %%       2. Delete the file.
               %%           - This is difficult without some kind of notation
               %%           somewhere that gives a hint about the missing file!
               %%
               [{20,  {call, ?MODULE, scribble,
                       %% This ?LET() creates the argument list for scribble().
                       %% Seq = 0 is OK, we'll add 1 to it later.
                       ?LET({Seq, Offset, Blob}, {nat(), largeint(), gen_blob()},
                            [Seq, abs(Offset), Blob])}} || ScribbleP])).

shortlong() ->
    oneof([shortterm, longterm]).

num_blobs() ->
    %% Don't make too many blobs, or else we're hit gmt_hlog's limit
    %% ?MAX_HUNK_HEADER_BYTES.
    choose(0, 3).

small_sleep() ->
    oneof([0, choose(10,30)]).

medium_sleep() ->
    oneof([0, choose(20, 60)]).

%% initial_state() :: symbolic_state().
%% Called in symbolic context.

initial_state() ->
    case file:read_file_info(?TEST_DIR) of
        {error, enoent} -> file:make_dir(?TEST_DIR);
        _               -> ok end,
    filelib:fold_files(?TEST_DIR, ".*", true,
                       fun(Path, _) -> file:delete(Path) end, x),
    #state{}.

%% precondition(S::symbolic_state(), C::call()) :: bool()
%% Called in symbolic (??) & runtime context.

precondition(_S, {call, _, start_server, _}) ->
    true;
precondition(_S, {call, _, write_hunk, _}) ->
    true;
precondition(_S, {call, _, sleep, [_]}) ->
    true;
precondition(_S, {call, _, advance_seqnum, [_, Incr]}) ->
    Incr /= 0;
precondition(_S, {call, _, async_advance_seqnum, [_, Incr, _]}) ->
    Incr /= 0;
precondition(_S, {call, _, async_sync, [_, _]}) ->
    true;
precondition(_S, {call, _, scribble, _}) ->
    true;
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
            %%_Key = hd(element(4, C)),
            %%io:format("BUMMER: ~p at ~p\n", [R, C]),
            false
    end.

postcondition2(_S, {call, _, start_server, _}, R) ->
    is_pid(R);
postcondition2(S, {call, _, write_hunk, [_, _ShortLong, TypeNum, CBs, Bs]}, R) ->
    case R of
        {ok, _File, _Off} ->
            true;
        {hunk_too_big, _Bytes} ->
            {H_Len, _} = ?MUT:create_hunk(TypeNum, CBs, Bs),
            H_Len + 32 > S#state.log_size_kb * 1024; % TODO fudge factor 32?
        _ ->
            false
    end;
postcondition2(_S, {call, _, sleep, [_]}, _R) ->
    true;
postcondition2(_S, {call, _, advance_seqnum, [_, _Incr]}, R) ->
    case R of
        {ok, _} -> true;
        error   -> false
    end;
postcondition2(_S, {call, _, async_advance_seqnum, [_, _, _]}, _R) ->
    true;
postcondition2(_S, {call, _, async_sync, [_, _]}, _R) ->
    true;
postcondition2(_S, {call, _, scribble, _}, _R) ->
    true;
postcondition2(_S, _Call, _R) ->
    io:format("\npostcondition: what the heck is this?  ~p\n", [_Call]),
    timer:sleep(1000),
    false.

%% next_state(S::symbolic_state(),R::var(),C::call()) :: symbolic_state()
%% Called in symbolic & runtime context.

next_state(S, Res, {call, _, start_server, [SizeKB, _, _]}) ->
    S#state{step = S#state.step + 1,
            server = Res, log_size_kb = SizeKB};
next_state(S, Res, {call, _, write_hunk, [_Server, ShortLong, TypeNum, CBs, Bs]}) ->
    case Res of
        {ok, FileNum, Offset} ->
            H = {FileNum, Offset, TypeNum, CBs, Bs},
            if ShortLong == shortterm ->
                    Hunks = S#state.hunks_short,
                    S#state{hunks_short = Hunks++[H]};
               ShortLong == longterm ->
                    Hunks = S#state.hunks_long,
                    S#state{hunks_long = Hunks++[H]}
            end;
        _ ->
            S
    end;
next_state(S, _Res, {call, _, sleep, _}) ->
    S;
next_state(S, _Res, {call, _, advance_seqnum, _}) ->
    S;
next_state(S, _Res, {call, _, async_advance_seqnum, _}) ->
    S;
next_state(S, _Res, {call, _, async_sync, _}) ->
    S;
next_state(S, _Res, {call, _, scribble, _}) ->
    S;
next_state(S, _Result, _Call) ->
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

start_server(FileLenMax, WriteSleep, SyncSleep) ->
    {ok, Pid} = ?MUT:start_link([{name, ?TEST_NAME},
                                 {file_len_max, FileLenMax*1024},
                                 {file_len_min, FileLenMax*1024},
                                 {debug_write_sleep, WriteSleep},
                                 {debug_sync_sleep, SyncSleep}]),
    Pid.

%%%%
%%%%

write_hunk(Server, ShortLong, TypeNum, CBs, Bs) ->
    HLogType = if ShortLong == shortterm -> metadata;
                  true                   -> bigblob_longterm
               end,
    ?MUT:write_hunk(Server, unused_locallog_name, HLogType, ShortLong, TypeNum, CBs, Bs).

sleep(SleepTime) ->
    timer:sleep(SleepTime).

advance_seqnum(Server, Incr) ->
    ?MUT:advance_seqnum(Server, Incr).

async_advance_seqnum(Server, Incr, SleepTime) ->
    spawn(fun() -> timer:sleep(SleepTime),
                   ?MUT:advance_seqnum(Server, Incr)
          end).

async_sync(Server, SleepTime) ->
    spawn(fun() -> timer:sleep(SleepTime),
                   ?MUT:sync(Server)
          end).

scribble(_, _, _) ->
    ok.

%%%%
%%%%

log_matches_state(S) ->
    Ffix_sign = fun(L) -> [setelement(1, X, abs(element(1,X))) || X <- L] end,
    {HsShort0, []} = wal_fold(shortterm, ?TEST_DIR),
    HsShort = Ffix_sign(HsShort0),
    StateShort = Ffix_sign(S#state.hunks_short),
    if (HsShort == StateShort) ->
            ok;
       true ->
            io:format("HsShort    = ~p\n", [HsShort]),
            io:format("StateShort = ~p\n", [StateShort])
    end,
    true = (HsShort == StateShort),
    {HsLong0, []} = wal_fold(longterm, ?TEST_DIR),
    HsLong = Ffix_sign(HsLong0),
    StateLong = Ffix_sign(S#state.hunks_long),
    if HsLong == StateLong ->
            ok;
       true ->
            io:format("HsLong: ~p\n", [HsLong]),
            io:format("S#state.hunks_long: ~p\n", [StateLong])
    end,
    true = (HsLong == StateLong),
    case lists:sort([element(1,X) || X <- HsShort ++ HsLong]) of
        [] ->
            ok;
        Sorted ->
            MaxSeq = lists:last(Sorted),
            %% NOTE: Constants 3 & 7 are cut-and-paste from gmt_hlog.erl
            MostestSeq = ?MUT:find_mostest_sequence_num(?TEST_DIR, 3, 7),
            %% We may have written only to one log file, but log has
            %% two files.  If we write to one and not the other, or if
            %% the last step that we do is an advance, then MaxSeq will
            %% be less than MostestSeq.
            true = (MaxSeq =< MostestSeq)
    end,
    true.

wal_fold(TermType, Dir) ->
    F = fun(HSum, FH, Acc) ->
                CBs = read_blobs(FH, HSum, md5, length(HSum#hunk_summ.c_len)),
                Bs = read_blobs(FH, HSum, non, length(HSum#hunk_summ.u_len)),
                true = ?MUT:md5_checksum_ok_p(HSum#hunk_summ{c_blobs = CBs}),
                T = {HSum#hunk_summ.seq,
                     HSum#hunk_summ.off,
                     HSum#hunk_summ.type,
                     CBs, Bs},
                Acc++[T]
        end,
    ?MUT:fold(TermType, Dir, F, []).

off_len_pairs(Start, SizeList) ->
    {Ps, _} = lists:foldl(
                fun(Size, {Xs, LastOffset}) ->
                        {[{LastOffset, Size}|Xs], LastOffset + Size}
                end, {[], Start}, SizeList),
    lists:reverse(Ps).

read_blobs(_FH, _HSum, _Type, 0) ->
    [];
read_blobs(FH, HSum, Type, Num) ->
    [?MUT:read_hunk_member_ll(FH, HSum, Type, Pos) || Pos <- lists:seq(1,Num)].

scribbles_cause_failure(_S, _Cmds, false) ->
    true;
scribbles_cause_failure(S, Cmds, true) ->
    %% Tell gmt_hlog:fold to be a bit less verbose.
    put(qc_verbose_hack_hlog_qc, true),

    case [Args || {set, _, {call, _, scribble, Args}} <- Cmds] of
        [] ->
            true;                               % No scribble commands
        ArgsList ->
            Files = filelib:fold_files(?TEST_DIR, ".*\\.HLOG", true,
                                       fun(Path, Acc) -> [Path|Acc] end, []),
            PathT = list_to_tuple(lists:sort(Files)),
            case PathT of
                {} ->
                    true;
                _ ->
                    case lists:usort(do_scribbles(ArgsList, PathT)) of
                        [true] ->
                            %% All of the scribbles were no-ops
                            io:format("NOOP"),
                            true;
                        _ ->
                            {_, ErrList1} = (catch wal_fold(shortterm, ?TEST_DIR)),
                            {_, ErrList2} = (catch wal_fold(longterm, ?TEST_DIR)),
                            io:format("ErrList1  ErrList2 ~p ~p\n", [ErrList1, ErrList2]),
                            %% There should be at least one error here.
                            case ErrList1 ++ ErrList2 of
                                [_|_] ->
                                    true;       % Got at least one error, yay!
                                [] ->
                                    %% No errors.  So, we assume that
                                    %% the scribble was in the middle
                                    %% of a un-checksummed blob.  So,
                                    %% check the log state again: we
                                    %% should see a discrepancy this
                                    %% time.
                                    (catch log_matches_state(S)) /= true
                            end
                    end
            end
    end.

do_scribbles(ArgsList, PathT) ->
    io:format("Num scribbles = ~p\n", [length(ArgsList)]),
    lists:map(
      fun([SeqNum, Offset, NewBlob]) ->
              Path = element((SeqNum rem size(PathT)) + 1, PathT),
              {ok, FI} = file:read_file_info(Path),
              Pos = Offset rem (FI#file_info.size + 1), % in case size = 0.
              {ok, FH} = file:open(Path, [binary, read, write]),
              io:format("Pread ~p at Pos ~p for ~p bytes\n", [Path, Pos, size(NewBlob)]),
              OldBlob = case (catch file:pread(FH, Pos, size(NewBlob))) of
                            {ok, BLB} -> BLB;
                            _         -> cant_match_any_binary_ha_ha
                        end,
              %% io:format("Skip Scribble ~p at ~p Pos ~p\n", [NewBlob, Path, Pos]),
              %% %%           ok = file:pwrite(FH, Pos, NewBlob),
              io:format("Use Scribble ~p at ~p Pos ~p\n", [NewBlob, Path, Pos]),
              ok = file:pwrite(FH, Pos, NewBlob),
              file:close(FH),
              %% io:format("Old Blob~p\n", [OldBlob]),
              OldBlob == NewBlob
      end, ArgsList).

%% Use Scribble <<"ff">> at "./zzz-hlog-qc/2/6/-000000000061.HLOG" Pos 10172
%% Pread "./zzz-hlog-qc/2/2/-000000000085.HLOG" at Pos 64 for 27 bytes
%% Use Scribble <<"RRRRRRRRRRRRRRRRRRRRRRRRRRR">> at "./zzz-hlog-qc/2/2/-000000000085.HLOG" Pos 64
%% Pread "./zzz-hlog-qc/2/6/-000000000061.HLOG" at Pos 1162 for 3 bytes
%% Use Scribble <<"¸¸¸">> at "./zzz-hlog-qc/2/6/-000000000061.HLOG" Pos 1162
%% Pread "./zzz-hlog-qc/000000000084.HLOG" at Pos 652 for 22 bytes
%% Use Scribble <<"NNNNNNNNNNNNNNNNNNNNNN">> at "./zzz-hlog-qc/000000000084.HLOG" Pos 652
%% Pread "./zzz-hlog-qc/000000000046.HLOG" at Pos 653 for 8 bytes
%% Use Scribble <<"ìììììììì">> at "./zzz-hlog-qc/000000000046.HLOG" Pos 653
%% make: *** [run-app1-interactive] Segmentation fault

%% .....Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 216 for 1 bytes
%% Use Scribble <<"Z">> at "./zzz-hlog-qc/000000000001.HLOG" Pos 216
%% Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 330 for 1 bytes
%% Use Scribble <<"Ö">> at "./zzz-hlog-qc/000000000001.HLOG" Pos 330
%% Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 83 for 1 bytes
%% Use Scribble <<"°">> at "./zzz-hlog-qc/000000000001.HLOG" Pos 83
%% Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 176 for 1 bytes
%% Use Scribble <<"?">> at "./zzz-hlog-qc/000000000001.HLOG" Pos 176

%% Crash dump was written to: erl_crash.dump
%% eheap_alloc: Cannot allocate 2147483700 bytes of memory (of type "heap_frag").
%% make: *** [run-app1-interactive] Aborted

%% ..Pread "./zzz-hlog-qc/3/3/-000000000002.HLOG" at Pos 1258 for 32 bytes
%% Use Scribble <<"                                ">> at "./zzz-hlog-qc/3/3/-000000000002.HLOG" Pos 1258
%% ErrList1  ErrList2 [] [{seq,-2,err,{badmatch,false}}]
%% .nn.nn.nnnnnnnnnnnnPread "./zzz-hlog-qc/1/7/-000000000090.HLOG" at Pos 3368 for 35 bytes
%% Use Scribble <<"ÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓÓ">> at "./zzz-hlog-qc/1/7/-000000000090.HLOG" Pos 3368
%% Pread "./zzz-hlog-qc/1/7/-000000000090.HLOG" at Pos 5299 for 10 bytes
%% Use Scribble <<"}}}}}}}}}}">> at "./zzz-hlog-qc/1/7/-000000000090.HLOG" Pos 5299
%% Pread "./zzz-hlog-qc/000000000091.HLOG" at Pos 1778 for 35 bytes
%% Use Scribble <<2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
%%                2,2,2>> at "./zzz-hlog-qc/000000000091.HLOG" Pos 1778
%% Pread "./zzz-hlog-qc/2/4/-000000000052.HLOG" at Pos 521 for 2 bytes
%% Use Scribble <<"¸¸">> at "./zzz-hlog-qc/2/4/-000000000052.HLOG" Pos 521
%% Pread "./zzz-hlog-qc/1/7/-000000000027.HLOG" at Pos 6285 for 24 bytes
%% Use Scribble <<"¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹¹">> at "./zzz-hlog-qc/1/7/-000000000027.HLOG" Pos 6285
%% Pread "./zzz-hlog-qc/3/6/-000000000047.HLOG" at Pos 2313 for 34 bytes
%% Use Scribble <<"ÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕÕ">> at "./zzz-hlog-qc/3/6/-000000000047.HLOG" Pos 2313
%% Pread "./zzz-hlog-qc/1/7/-000000000027.HLOG" at Pos 3540 for 21 bytes
%% Use Scribble <<"RRRRRRRRRRRRRRRRRRRRR">> at "./zzz-hlog-qc/1/7/-000000000027.HLOG" Pos 3540
%% Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 443 for 0 bytes
%% Use Scribble <<>> at "./zzz-hlog-qc/000000000001.HLOG" Pos 443
%% Pread "./zzz-hlog-qc/000000000065.HLOG" at Pos 2496 for 21 bytes
%% Use Scribble <<"ooooooooooooooooooooo">> at "./zzz-hlog-qc/000000000065.HLOG" Pos 2496
%% Pread "./zzz-hlog-qc/2/2/-000000000064.HLOG" at Pos 70 for 16 bytes
%% Use Scribble <<"´´´´´´´´´´´´´´´´">> at "./zzz-hlog-qc/2/2/-000000000064.HLOG" Pos 70
%% Pread "./zzz-hlog-qc/3/6/-000000000047.HLOG" at Pos 6226 for 4 bytes
%% Use Scribble <<"ºººº">> at "./zzz-hlog-qc/3/6/-000000000047.HLOG" Pos 6226
%% make: *** [run-app1-interactive] Segmentation fault

%% .
%% =GMT ERR REPORT==== 30-Jul-2009::21:05:45 ===
%% fold_a_file: seq -2 offset 0: {error,unknown_file_header,
%%                                      {ok,<<"LogVersion1L">>}}
%% .Pread "./zzz-hlog-qc/3/3/-000000000002.HLOG" at Pos 115 for 1 bytes
%% Use Scribble <<"·">> at "./zzz-hlog-qc/3/3/-000000000002.HLOG" Pos 115
%% Pread "./zzz-hlog-qc/3/3/-000000000002.HLOG" at Pos 127 for 1 bytes
%% Use Scribble <<"0">> at "./zzz-hlog-qc/3/3/-000000000002.HLOG" Pos 127

%% Crash dump was written to: erl_crash.dump
%% eheap_alloc: Cannot allocate 3087007804 bytes of memory (of type "heap_frag").
%% make: *** [run-app1-interactive] Aborted

%% ..Pread "./zzz-hlog-qc/3/3/-000000000002.HLOG" at Pos 32 for 10 bytes
%% Use Scribble <<"²²²²²²²²²²">> at "./zzz-hlog-qc/3/3/-000000000002.HLOG" Pos 32
%% Pread "./zzz-hlog-qc/3/3/-000000000002.HLOG" at Pos 579 for 9 bytes
%% Use Scribble <<"\e\e\e\e\e\e\e\e\e">> at "./zzz-hlog-qc/3/3/-000000000002.HLOG" Pos 579
%% Pread "./zzz-hlog-qc/000000000001.HLOG" at Pos 880 for 1 bytes
%% Use Scribble <<"í">> at "./zzz-hlog-qc/000000000001.HLOG" Pos 880
%% make: *** [run-app1-interactive] Segmentation fault

%% .nnnPread "./zzz-hlog-qc/2/5/-000000000004.HLOG" at Pos 513 for 6 bytes
%% Use Scribble <<"©©©©©©">> at "./zzz-hlog-qc/2/5/-000000000004.HLOG" Pos 513
%% Pread "./zzz-hlog-qc/000000000005.HLOG" at Pos 32 for 23 bytes
%% Use Scribble <<"©©©©©©©©©©©©©©©©©©©©©©©">> at "./zzz-hlog-qc/000000000005.HLOG" Pos 32

-endif. %% -ifdef(QC).
