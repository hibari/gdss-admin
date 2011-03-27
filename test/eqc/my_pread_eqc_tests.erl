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
%%% File    : my_pread_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(my_pread_eqc_tests).

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

-include_lib("kernel/include/file.hrl").

-compile(export_all).

run() ->
    run(500).

run(NumTests) ->
    ?GMTQC:module({numtests,NumTests}, ?MODULE).

-record(state, {
          size_f1,
          size_f2,
          fh1,
          fh2,
          standard_fh1,
          standard_fh2,
          cur_offset,
          cur_sym_fh
         }).

prop_pread() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {_Hist, S, Res} = run_commands(?MODULE, Cmds),
                ?WHENFAIL(begin
                              io:format("S = ~p\nR = ~p, Hist = ~p\n",
                                        [S, Res, _Hist]),
                              ok
                          end,
                          begin
                              [catch file:close(X) || X <- [S#state.fh1, S#state.fh2, S#state.standard_fh1, S#state.standard_fh2]],
                              Res == ok
                          end)
            end).

initial_state() ->
    File1 = "deleteme."++ atom_to_list(?MODULE) ++ ".1",
    File2 = "deleteme."++ atom_to_list(?MODULE) ++ ".2",
    B = list_to_binary(lists:seq(0,255)),
    B2 = lists:duplicate(77, B),
    file:write_file(File1, B2),
    file:write_file(File2, ["Hello, world!", B2]),
    {ok, FI1} = file:read_file_info(File1),
    {ok, FI2} = file:read_file_info(File2),
    [FH1, FH2, StandardFH1, StandardFH2] =
        [element(2, file:open(F, [read, binary])) ||
            F <- [File1, File2, File1, File2]],
    #state{size_f1 = FI1#file_info.size,
           size_f2 = FI2#file_info.size,
           fh1 = FH1, fh2 = FH2,
           standard_fh1 = StandardFH1, standard_fh2 = StandardFH2,
           cur_offset = 0, cur_sym_fh = a
          }.

command(S) ->
    frequency(
      [
       {10, {call, ?MODULE, change_fd, [gen_sym_fd()]}},
       {50, {call, ?MODULE, read_fd, [S, gen_offset(S), gen_len(S)]}}
      ]).

gen_sym_fd() ->
    oneof([a, b]).

gen_offset(S) ->
    ?LET(Perc, choose(1, 100),
         if Perc < 75 -> S#state.cur_offset;
            true      -> ?LET(MAX, oneof([S#state.size_f1, S#state.size_f2]),
                              choose(1, MAX))
         end).

gen_len(_S) ->
    frequency([{ 5, 0},
               {50, choose(1, 1024)},
               {50, choose(1024, 8192)}
              ]).

precondition(_S, _) ->
    true.

postcondition(_S, {call, _, change_fd, _}, _R) ->
    true;
postcondition(S, {call, _, read_fd, [_S, Offset, Len]}, {ok, Bin1}) ->
    StandardFH = get_standard_fh(S),
    {ok, Bin2} = file:pread(StandardFH, Offset, Len),
    if Bin1 /= Bin2 -> io:format("BUMMER: Bin1 ~p /= Bin2 ~p\n", [size(Bin1), size(Bin2)]),
                       io:format("BUMMER: Bin1 ~P /= Bin2 ~P\n", [Bin1, 10, Bin2, 10]),
                       timer:sleep(3000);
       true -> ok
    end,
    Bin1 == Bin2;
postcondition(S, {call, _, read_fd, [_S, Offset, Len]}, eof) ->
    StandardFH = get_standard_fh(S),
    try file:pread(StandardFH, Offset, Len) of
        eof        -> true;
        {ok, <<>>} -> true
    catch
        _:_X       -> io:format("XXXYYYZZZ: eof vs std ~p", [_X]),false
    end;
postcondition(S, {call, _, read_fd, [_S, Offset, Len]}, R) ->
    StandardFH = get_standard_fh(S),
    R == file:pread(StandardFH, Offset, Len).

next_state(S, _R, {call, _, change_fd, [Sym]}) ->
    S#state{cur_sym_fh = Sym};
next_state(S, _R, {call, _, read_fd, [_S, Offset, Len]}) ->
    S#state{cur_offset = Offset + Len}.

%%%

change_fd(_Sym) ->
    io:format("c"),
    ok.

read_fd(S, Offset, Len) ->
    %%io:format("r<~p,~p,~p", [S#state.cur_sym_fh, Offset, Len]),
    FH = get_test_fh(S),
    gmt_hlog:my_pread_start(),
    Res = gmt_hlog:my_pread(FH, Offset, Len, 0),
    Res.

%%%

get_test_fh(#state{cur_sym_fh = a} = S) -> S#state.fh1;
get_test_fh(#state{cur_sym_fh = b} = S) -> S#state.fh2.

get_standard_fh(#state{cur_sym_fh = a} = S) -> S#state.standard_fh1;
get_standard_fh(#state{cur_sym_fh = b} = S) -> S#state.standard_fh2.

get_size(#state{cur_sym_fh = a} = S) -> S#state.size_f1;
get_size(#state{cur_sym_fh = b} = S) -> S#state.size_f2.

-endif. %% -ifdef(GMTQC).
