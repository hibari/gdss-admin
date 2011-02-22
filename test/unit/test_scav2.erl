%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
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
%%% File    : test_scav2.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(test_scav2).

-define(CTAB, ets).

-export([test_all/0, node2/0, node3/0]).
-export([test_scav/1, test_get_keys/1]).

-record(hunk_summ, {seq,off,type,len,first_off,c_len,u_len,md5s,c_blobs,u_blobs}).

-define(BRICK__GET_MANY_FIRST, '$start_of_table').
-define(BIGKEY, <<"big one">>).
-define(BIGSIZE, (1000*1000)).
-define(TOUTSEC, 5*1000).

test_all() ->
    register(test0, self()),
    %% gmt_util:dbgon(brick_ets, scavenger_get_many),
    %% gmt_util:dbgon(brick_ets),
    %% gmt_util:dbgadd(gmt_hlog_common, spawn_future_tasks_after_dirty_buffer_wait),

    Tab = init0(),

    test_get_keys(Tab),
    %% test_scav(Tab),

    ok.

test_get_keys(Tab) ->
    NumKeys = 10*1000,
    BinSize = 100,
    Percent = 50,

    TestFun1 = fun() -> get_keys0(Tab,2) end,

    %%ThreadsList = [0,200,400,600,800,1000],
    StartKeysList = [0, NumKeys, 2*NumKeys, 3*NumKeys,
                     4*NumKeys, 5*NumKeys, 6*NumKeys,
                     7*NumKeys, 8*NumKeys, 9*NumKeys,
                     10*NumKeys, 11*NumKeys, 12*NumKeys, 13*NumKeys,
                     14*NumKeys, 15*NumKeys, 16*NumKeys,
                     17*NumKeys, 18*NumKeys, 19*NumKeys,
                     20*NumKeys, 21*NumKeys, 22*NumKeys, 23*NumKeys,
                     24*NumKeys, 25*NumKeys, 26*NumKeys,
                     27*NumKeys, 28*NumKeys, 29*NumKeys
                    ],
    ThreadsList = [10,20,40,80,160,320],
    %% StartKeysList = [0],

    start_test(TestFun1,NumKeys,BinSize,Percent,Tab,
               ThreadsList,StartKeysList),
    ok.

test_scav(Tab) ->

    %% NumKeys = 16*1000,
    %% BinSize = 100*1000,
    %% NumKeys = 160*1000,

    NumKeys = 10*1000,
    BinSize = 100*1000,
    Percent = 20,
    TestFun = fun test0/0,

    ThreadsList = [0],
    StartKeysList = [0],

    start_test(TestFun,NumKeys,BinSize,Percent,Tab,
               ThreadsList,StartKeysList).


start_test(TestFun,NumKeys,BinSize,_Percent,Tab,
           ThreadsList,StartKeysList) ->
    lists:foreach(
      fun(StartKey) ->
              {NewNumKeys,Keys} =
                  init1(NumKeys,BinSize,StartKey,Tab),

              %% eprof:start(),
              %% eprof:start_profiling([testtable_ch1_b1],{brick_server,'_','_'}),
              %% eprof:start_profiling([testtable_ch1_b1],{brick_ets,'_','_'}),
              %% fprof:trace([start, {procs, whereis(testtable_ch1_b1)}]),
              lists:foreach(
                fun(Threads) ->
                        timer:sleep(10*1000),
                        io:format(":::===starting test with ~p keys, ~p threads, ~p bytes binary===~n",
                                  [NewNumKeys,Threads,BinSize]),

                        Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
                        TooOld = gen_server:call({Br, node2()}, {get_too_old}),
                        io:format(":::too old1: ~p~n", [TooOld]),

                        ClientPIDs =
                            if Threads > 0 ->
                                    start_clients(NewNumKeys,Tab,Keys,Threads,BinSize),
                                    timer:sleep(5000),
                                    receive {get2_done, GetL} ->
                                            receive {set2_done, SetL} ->
                                                    %% io:format(":::setl=~p~n",[SetL]),
                                                    %% io:format(":::getl=~p~n",[GetL]),
                                                    SetL++GetL
                                            end
                                    end;
                               true ->
                                    {none, none}
                            end,

                        TestFun(),

                        TooOld2 = gen_server:call({Br, node2()}, {get_too_old}),
                        io:format(":::too old2: ~p~n", [TooOld2]),
                        if Threads > 0 ->
                                io:format(":::# of clients: ~p~n", [length(ClientPIDs)]),
                                lists:foreach(fun(P)->
                                                      P ! stop_client
                                              end,
                                              ClientPIDs),
                                sync_messages(client_done, length(ClientPIDs)),
                                ok;
                           true ->
                                noop
                        end,
                        TooOld3 = gen_server:call({Br, node2()}, {get_too_old}),
                        io:format(":::too old3: ~p~n", [TooOld3])
                end,
                ThreadsList)

              %% ,eprof:stop_profiling(),
              %% eprof:log("/tmp/profile1"),
              %% eprof:analyze(total),
              %% eprof:stop()
              %% ,fprof:trace([stop]),
              %% fprof:profile(),
              %% fprof:analyse(),
              %% fprof:stop()
      end,
      StartKeysList),
    ok.


get_keys0(Tab,N) ->
    Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
    lists:foreach(fun(_) ->
                          %%			  get_keys00(Tab,false),
                          get_keys00ctab(Tab,true),
                          sync_checkp(Br),
                          get_keys00(Tab,true),
                          sync_checkp(Br)
                  end, lists:seq(1,N)),
    ok.

sync_checkp(Br) ->
    case checkp(Br) of
        undefined ->
            io:format(":::checkpoint: finished~n"),
            done;
        _Pid ->
            io:format(":::checkpoint: ~p~n", [_Pid]),
            timer:sleep(5000),
            sync_checkp(Br)
    end.

checkp(Br) ->
    {ok,Stat} = gen_server:call({Br, node2()}, {status}),
    Imp = proplists:get_value(implementation,Stat),
    proplists:get_value(checkpoint,Imp).



get_keys00ctab(Tab,StartCheckpoint) ->
    if StartCheckpoint ->
            Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
            brick_server:checkpoint(Br,node());
       true ->
            noop
    end,

    io:format("::: starting get_keys/ctab(checkpoint=~p) ------------~n",
              [StartCheckpoint]),
    StartT = erlang:now(),

    Keys = get_many_all(Tab),

    EndT = erlang:now(),
    Diff = timer:now_diff(EndT,StartT),
    io:format("::: get_keys/ctab finished in ~B.~3..0B seconds-------~n",
              [Diff div (1000*1000), (Diff div 1000) rem 1000]),
    io:format("::: get_keys/ctab returned ~p keys~n",
              [length(Keys)]),
    ok.

get_keys00(Tab,StartCheckpoint) ->
    Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
    Fs = [get_many_raw_storetuples],
    First = ?BRICK__GET_MANY_FIRST,
    F_k2d = fun({_BrickName, _Key, _TS, {0, 0}}, Dict) ->
                    Dict;
               ({BrickName, Key, TS,
                 {SeqNum, Offset},
                 ValLen, ExpTime, Flags},Dict) ->
                    {Bytes, L} = case dict:find(SeqNum, Dict) of
                                     {ok, {B_, L_}} -> {B_, L_};
                                     error          -> {0, []}
                                 end,

                    Tpl = {Offset,BrickName,Key,TS,
                           ValLen,ExpTime, Flags},
                    dict:store(SeqNum,
                               {Bytes + ValLen, [Tpl|L]}, Dict)
            end,
    KeysD = allkeys,
    put(KeysD,0),
    F_lump = fun (Dict) ->
                     List = dict:to_list(Dict),
                     lists:foreach(
                       fun({_Seq, {_Bytes, Tpls}}) ->
                               N = get(KeysD),
                               put(KeysD, N+length(Tpls))
                       end, List),
                     dict:new()
             end,
    if StartCheckpoint ->
            brick_server:checkpoint(Br,node());
       true ->
            noop
    end,

    io:format("::: starting get_keys(checkpoint=~p) ------------~n",
              [StartCheckpoint]),
    StartT = erlang:now(),
    case catch brick_ets:scavenger_get_keys(Br, Fs, First,
                                            F_k2d, F_lump) of
        ok ->
            noop;
        {'EXIT', {timeout, _}} ->
            io:format(":::get_keys0: timeout ################~n");
        _E ->
            io:format(":::get_keys0: error(~p):::~n", [_E])
    end,
    EndT = erlang:now(),
    Diff = timer:now_diff(EndT,StartT),
    io:format("::: get_keys finished in ~B.~3..0B seconds-------~n",
              [Diff div (1000*1000), (Diff div 1000) rem 1000]),
    io:format("::: get_keys returned ~p keys~n",
              [get(KeysD)]),

    ok.

init0() ->
    MyBase = "testtable",
    %% SchemaFile = MyBase ++ "schema.txt",
    Tab = list_to_atom(MyBase),

    create_table(Tab),
    timer:sleep(10000),

    EtsName = ctab(Tab),
    io:format(":::ctab: ~p~n",[EtsName]),
    Tab.

ctab(Tab) ->
    Br = list_to_atom(atom_to_list(Tab)++"_"++"ch1_b1"),
    {ok,Stat} = gen_server:call({Br, node2()}, {status}),
    Imp = proplists:get_value(implementation,Stat),
    proplists:get_value(ets_table_name, Imp).

init1(Num,BinSize,StartKey,Tab) ->
    io:format(":::starting init1=========================~n"),
    check_log("./hlog.commonLogServer"),

    Bin = list_to_binary(lists:duplicate(BinSize,$t)),
    io:format(":::blob size = ~p bytes:::~n", [size(Bin)]),
    Keys = lists:seq(StartKey,StartKey+Num-1),

    io:format(":::adding ~p keys----------------------~n", [Num]),
    ok = add(Tab,split(Keys,100),Bin), %% use only 100 threads
    check_log("./hlog.commonLogServer"),

    {StartKey+Num,Keys}.


test0() ->
    io:format(":::moving to the longterm----------------------~n"),
    timer:sleep(10*1000), %% waite for the move to finish
    check_log("./hlog.commonLogServer"),

    io:format(":::starting scavenger----------------------~n"),
    gmt_hlog_common:start_scavenger_commonlog([]),


    timer:sleep(1*1000), %% wait scav to start
    scav_wait(),
    io:format(":::--- scavenger finished --------------------~n"),

    io:format(":::============================================~n"),
    check_log("./hlog.commonLogServer"),
    io:format(":::============================================~n"),
    ok.


start_clients(_Num,Tab,Keys,Threads,BinSize) ->
    Bin = list_to_binary(lists:duplicate(BinSize,$t)),
    KeysSplit = split(Keys, Threads),

    Parent = self(),
    spawn(
      fun() ->
              io:format(":::setting keys again while running scavenger---~n"),
              PIDL = set(Tab,KeysSplit,Bin,Parent),
              io:format(":::--- set started --------------------~n"),
              Parent ! {set2_done, PIDL}
      end),
    spawn(
      fun() ->
              io:format(":::getting keys while running scavenger---~n"),
              PIDL = get(Tab,KeysSplit,Parent),
              io:format(":::--- get started --------------------~n"),
              Parent ! {get2_done, PIDL}
      end).



add(Tab,KeysSplit,Bin) ->
    Parent = self(),
    AddFun =
        fun(Keys) ->
                fun() ->
                        lists:foreach(
                          fun(N) ->
                                  B=list_to_binary(
                                      integer_to_list(N)),
                                  case catch brick_simple:add(Tab, B, Bin, ?TOUTSEC) of
                                      ok ->
                                          noop;
                                      {'EXIT', {timeout, _}} ->
                                          io:format(":::add timeout:::~n"),
                                          timer:sleep(10*1000);
                                      E ->
                                          io:format(":::add error: ~p:::~n",[E]),
                                          timer:sleep(10*1000)
                                  end
                          end, Keys),
                        Parent ! add_done
                end
        end,

    PidList =
        lists:foldl(
          fun(Keys,Acc) ->
                  Pid = spawn(AddFun(Keys)),
                  [Pid|Acc]
          end,
          [], KeysSplit),
    sync_messages(add_done, length(PidList)).

sync_messages(_Msg,0) ->
    io:format(":::~p sync done:::~n", [_Msg]),
    ok;
sync_messages(Msg,Num) ->
    %%    io:format(":::~p/~p syncing:::~n", [Msg,Num]),
    receive Msg ->
            sync_messages(Msg,Num-1)
    end.

set(Tab,KeysSplit,Bin,PParent) ->
    Fun0 = fun(N) ->
                   B=list_to_binary(integer_to_list(N)),
                   brick_simple:set(Tab, B, Bin, ?TOUTSEC)
           end,
    do(Tab,KeysSplit,PParent,Fun0,set).

get(Tab,KeysSplit,PParent) ->
    Fun0 = fun(N) ->
                   B=list_to_binary(integer_to_list(N)),
                   brick_simple:get(Tab, B, ?TOUTSEC)
           end,
    do(Tab,KeysSplit,PParent,Fun0,get).

do(_Tab,KeysSplit,PParent,Fun0,Name) ->
    _Parent = self(),
    SetFun =
        fun(Keys) ->
                fun() ->
                        lists:foreach(
                          fun(N) ->
                                  receive
                                      stop_client ->
                                          %% io:format(":::stop_client:::~n"),
                                          PParent ! client_done,
                                          exit(normal)
                                  after 0 ->
                                          _B=list_to_binary(
                                               integer_to_list(N)),
                                          case catch Fun0(N) of
                                              ok ->
                                                  noop; %% for set
                                              {ok,_,_} ->
                                                  noop; %% for get
                                              {'EXIT', {timeout, _}} ->
                                                  io:format(":::~p: timeout:::~n",[Name]);
                                              E ->
                                                  io:format(":::~p: error: ~p:::~n",[Name,E])
                                          end
                                  end
                          end, lists:flatten(lists:duplicate(100,Keys))),
                        io:format(":::client ~p finished~n",[self()]),
                        PParent ! client_done
                end
        end,
    PidList =
        lists:foldl(
          fun(Keys,Acc) ->
                  Pid = spawn(SetFun(Keys)),
                  [Pid|Acc]
          end,
          [], KeysSplit),
    PidList.

check_log0(Dir,SorL) ->
    gmt_hlog:fold(SorL, Dir,
                  fun(#hunk_summ{c_len=CL0, u_len=UL0},_FH,
                      {CCount,CBytes,UCount,UBytes}=_TTT) ->
                          CL =
                              case CL0 of
                                  CL0 when is_list(CL0) -> CL0;
                                  _ -> []
                              end,
                          UL =
                              case UL0 of
                                  UL0 when is_list(UL0) -> UL0;
                                  _ -> []
                              end,

                          {CCount + length(CL),
                           CBytes + lists:foldl(fun (X,Acc) ->
                                                        X+Acc
                                                end,0,
                                                CL),
                           UCount + length(UL),
                           UBytes + lists:foldl(fun (X,Acc) ->
                                                        X+Acc
                                                end,0,
                                                UL)
                          }
                  end, {0,0,0,0}).

check_log(Dir) ->
    {{LongC, LongCBytes, LongU, LongUBytes},[]} =
        check_log0(Dir,longterm),

    io:format("~n:::~p cblobs(~p bytes total):::long~n",
              [LongC, LongCBytes]),
    io:format("~n:::~p ublobs(~p bytes total):::long~n",
              [LongU, LongUBytes]),

    {{ShortC, ShortCBytes, ShortU, ShortUBytes},[]} =
        check_log0(Dir,shortterm),

    io:format("~n:::~p cblobs(~p bytes total):::short~n",
              [ShortC, ShortCBytes]),
    io:format("~n:::~p ublobs(~p bytes total):::short~n",
              [ShortU, ShortUBytes]),

    ok.



create_table(Tab) ->
    ChDescr = brick_admin:make_chain_description(
                Tab, 1, [node()]),
    brick_admin:add_table(Tab, ChDescr,
                          [{bigdata_dir,"cwd"},
                           {do_logging,true},
                           {do_sync,true}
                          ]).

scav_wait() ->
    F_excl_wait =
        fun() ->
                noop
        end,
    F_excl_go =
        fun() ->
                io:format(":::scavenger finished----------~n")
        end,
    brick_ets:really_cheap_exclusion(excl_atom(),
                                     F_excl_wait, F_excl_go),
    unregister(excl_atom()). %% for next iteration of test0()

split(List,N) ->
    NN = length(List) div N,
    lists:foldl(fun (X,[]) ->
                        [[X]];
                    (X,Acc) when (X rem NN)==0 ->
                        [[X]|Acc];
                    (X,[Car|Cdr]) ->
                        [[X|Car]|Cdr]
                end, [], List).

excl_atom() ->
    the_scavenger_proc.

node2() ->
    list_to_atom("gdss_dev2@"++gmt_util:node_right()).

node3() ->
    list_to_atom("gdss_dev3@"++gmt_util:node_right()).

%%%%%%%%%%%%%%%%%%%%%%%%%%

get_many_all(Tab) ->
    CTab = ctab(Tab),
    Start = ?CTAB:first(CTab),
    get_many_all0(CTab,Start,[]).

get_many_all0(CTab,Start,Acc) ->
    %%    io:format(":::ctab ~p/~p/~p~n", [CTab,Start,Acc]),
    Max = 100,
    case get_many_ctab2(CTab, Start, Max,[]) of
        {Tuples, false} ->
            Tuples++Acc;
        {[H|_]=Tuples, true} ->
            %%	    io:format(":::tuples: ~p~n",[Tuples]),
            get_many_all0(CTab,?CTAB:next(CTab, element(1,H)),
                          Tuples++Acc)
    end.

get_many_ctab2(_CTab, '$end_of_table', _MaxNum, Acc) ->
    {Acc, false};
get_many_ctab2(_CTab, _Key, 0, Acc) ->
    {Acc, true};
get_many_ctab2(CTab, Key, MaxNum, Acc) ->
    [Tuple] = ?CTAB:lookup(CTab, Key),
    get_many_ctab2(CTab,
                   ?CTAB:next(CTab, Key), MaxNum - 1, [Tuple|Acc]).
