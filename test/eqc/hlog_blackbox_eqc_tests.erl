%%%----------------------------------------------------------------------
%%% Copyright (c) 2010-2017 Hibari developers.  All rights reserved.
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
%%% File    : hlog_blackblox_eqc_tests.erl
%%% Purpose :
%%%----------------------------------------------------------------------

-module(hlog_blackbox_eqc_tests).

-ifdef(QC).

-eqc_group_commands(false).
-include_lib("qc/include/qc_statem.hrl").

-export([run/0, run/1]).
-export([start_noshrink/0, start_noshrink/1]).
-export([run_parallel/0, run_parallel/1]).
-export([run_parallel_noshrink/0, run_parallel_noshrink/1]).
-export([sample_commands/0, sample_commands/1, prop_commands/0, prop_commands/1]).
-export([counterexample_commands/0, counterexample_commands/1, counterexample_commands/2]).
-export([counterexample_commands_read/1, counterexample_commands_write/1, counterexample_commands_write/2]).

%% qc_statem Callbacks
%% -behaviour(qc_statem).
-export([command/1]).
-export([initial_state/0, initial_state/1, next_state/3, invariant/1, precondition/2, postcondition/3]).
-export([init/0, init/1, stop/2, aggregate/1]).

-export([restart/1]).
-export([stop/1]).
-export([kill/1]).
-export([write_hunk/7]).
-export([read_hunk/4,read_hunk/5]).
-export([advance_seqnum/2]).
-export([move_seq_to_longterm/3]).
-export([get_all_seqnums/1]).
-export([get_current_seqnum/1]).
-export([get_current_seqnum_and_file_position/1]).
-export([get_current_seqnum_and_offset/1]).
-export([fold/4]).

-define(API_MOD, gmt_hlog).
-define(INIT_MOD, ?MODULE).

-define(SERVERNAMES, [a,b,c]).
-define(REGNAMES, [a_hlog,b_hlog,c_hlog]).
-define(DIRNAMES, ["hlog.a","hlog.b","hlog.c"]).
-define(FILELENFACTOR, 32).

-type proplist() :: proplists:proplist().

-record(state,
        {parallel=false
         , pids=[]                %% [pid()]
         , killed=[]              %% [pid()]
         , servers=[]             %% [server()]
         , files=[]               %% [file()]
        }).

-record(hunk,
        {type                     %% metadata|bigblob|bigblob_longterm
         , typenum                %% integer()
         , cblobs=[]              %% list(iolist())
         , ublobs=[]              %% list(iolist())
         , len                    %% bytes()
         , seqnum                 %% seqnum()
         , offset                 %% offset()
        }).

-record(log,
        {type                     %% short|long
         , seqnum=0               %% the "current" seqnum()
         , offset=0               %% the "current" offset()
         , hunks=[]               %% [hunk()]
        }).

-record(server,
        {pid                      %% pid()
         , key                    %% atom()
         , props=[]               %% props()
         , short=#log{type=short} %% log()
         , long=#log{type=long}   %% log()
        }).

%% -record(file,
%%         {fd                       %% fd()
%%          , key                    %% atom()
%%          , props=[]               %% props()
%%          , log=#log{}             %% log()
%%         }).


%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

run() ->
    run(500).

run(NumTests) ->
    qc_statem:qc_run(?MODULE, NumTests, [{parallel,false}]).

start_noshrink() ->
    start_noshrink(500).

start_noshrink(NumTests) ->
    qc_statem:qc_run(?MODULE, NumTests, [noshrink, {parallel,false}]).

run_parallel() ->
    run_parallel(500).

run_parallel(NumTests) ->
    qc_statem:qc_run(?MODULE, NumTests, [{parallel,true}]).

run_parallel_noshrink() ->
    run_parallel_noshrink(500).

run_parallel_noshrink(NumTests) ->
    qc_statem:qc_run(?MODULE, NumTests, [noshrink, {parallel,true}]).

%% sample commands
sample_commands() ->
    sample_commands([]).

sample_commands(Options) ->
    qc_statem:qc_sample(?MODULE, Options).

%% prop commands
prop_commands() ->
    prop_commands([]).

prop_commands(Options) ->
    error_logger:delete_report_handler(error_logger_tty_h),
    qc_statem:qc_prop(?MODULE, Options).

%% counterexample commands
counterexample_commands() ->
    counterexample_commands([]).

counterexample_commands(Options) ->
    counterexample_commands(Options, ?QC:counterexample()).

counterexample_commands(Options, CounterExample) ->
    qc_statem:qc_counterexample(?MODULE, Options, CounterExample).

%% counterexample commands read
counterexample_commands_read(FileName) ->
    {ok, CounterExample} = file:consult(FileName),
    counterexample_commands(CounterExample).

%% counterexample commands write
counterexample_commands_write(FileName) ->
    counterexample_commands_write(FileName, ?QC:counterexample()).

counterexample_commands_write(FileName, CounterExample) ->
    file:write_file(FileName, io_lib:format("~p.", [CounterExample])).

%%%----------------------------------------------------------------------
%%% CALLBACKS - qc_statem
%%%----------------------------------------------------------------------

-spec init() -> ok.
init() ->
    commands_setup(true),
    ok.

-spec init(#state{}) -> ok.
init(_S) ->
    commands_setup(true),
    ok.

command(_S) ->
    %% DEBUG io:format("~p ~p~n", [TypeName]),
    ?LET(TypeName,command_typenamegen(_S),
         ?LET(Type,command_typegen(_S,TypeName),
              begin
                  if is_atom(Type) ->
                          {call,?MODULE,Type,[]};
                     is_tuple(Type) ->
                          [Fun|Args] = tuple_to_list(Type),
                          {call,?MODULE,Fun,Args}
                  end
              end)).


%%
%% Functions tested by test model:
%%
%%   advance_seqnum *
%%   create_hunk *
%%   fold *
%%   get_all_seqnums
%%   get_current_seqnum
%%   get_current_seqnum_and_file_position
%%   get_current_seqnum_and_offset
%%   kill (SEQUENTIAL ONLY)
%%   move_seq_to_longterm *
%%   read_hunk
%%   stop * (SEQUENTIAL ONLY)
%%   sync * (AUTOMAGICALLY)
%%   write_hunk *
%%
%% NOTE: '*' indicates functions called by brick_ets.erl
%%


%%
%% These functions have not been added to the test model:
%%
%%   find_current_log_files *
%%   find_current_log_seqnums *
%%   find_longterm_log_seqnums
%%   get_proplist *
%%   log_file_path *
%%   log_name2data_dir *
%%   md5_checksum_ok_p *
%%   open_log_file *
%%   read_bigblob_hunk_blob *
%%   read_hunk_member_ll *
%%   read_hunk_summary *
%%   read_hunk_summary_ll *
%%   write_log_header *
%%
%% NOTE: '*' indicates functions called by brick_ets.erl
%%


command_typenamegen(#state{parallel=Parallel,pids=Pids}=S) ->
    ?LET(Hunks,all_good_hunks(S),
         oneof([restart
               ]
               ++ [ oneof([ oneof([stop, kill]) || not Parallel ]
                          ++ [ write_hunk
                               , advance_seqnum
                               , fold
                             ]
                          ++ [ oneof([get_all_seqnums
                                      , get_current_seqnum
                                      , get_current_seqnum_and_file_position
                                      , get_current_seqnum_and_offset
                                     ])
                             ]
                          ++ [ read_hunk
                               || [] =/= Hunks ]
                          ++ [ move_seq_to_longterm
                               || [] =/= Hunks ]
                         )
                    || [] =/= Pids ]
              )).

command_typegen(S,restart) ->
    %% restart(props())
    {restart, command_typegen(S,props)};
command_typegen(S,stop) ->
    %% stop(server())
    {stop, command_typegen(S,server)};
command_typegen(S,kill) ->
    %% kill(server())
    {kill, command_typegen(S,server)};
command_typegen(S,write_hunk) ->
    %% write_hunk(server(), LocalBrickName::atom(), HLogType::metadata|bigblob|bigblob_longterm, Key::iolist(), TypeNum::integer(), CBlobs::list(iolist()), UBlobs::list(iolist))
    ?SIZED(Size,
           ?LET(N,choose(1,erlang:max(1,Size)),
                %% ASSUMPTION: read_hunk assumes the length of either
                %% cblobs or ublobs is non-empty
                oneof([{write_hunk
                        , command_typegen(S,server)
                        , undefined %% not used
                        , oneof([metadata, bigblob, bigblob_longterm])
                        , binary()
                        , int()
                        , oneof([[binary(1,N)], [binary(1,N)|list(binary(1,N))]])
                        , list(binary(1,N))
                       }
                       , {write_hunk
                          , command_typegen(S,server)
                          , undefined %% not used
                          , oneof([metadata, bigblob, bigblob_longterm])
                          , binary()
                          , int()
                          , list(binary(1,N))
                          , oneof([[binary(1,N)], [binary(1,N)|list(binary(1,N))]])
                         }
                      ])));
command_typegen(S,read_hunk) ->
    %% read_hunk(server(), seqnum(), offset())
    %% read_hunk(server(), seqnum(), offset(), lenhintORxformfun())
    %% read_hunk(server(), seqnum(), offset(), lenhint(), xformfun())
    Hunks = all_good_hunks(S),
    ?LET({#server{pid=Pid},Log,#hunk{seqnum=SeqNum,offset=Offset,len=Len}},oneof(Hunks),
         begin
             LenHint = oneof([Len,command_typegen(S,lenhint)]),
             oneof([{read_hunk
                     , Pid
                     , SeqNum
                     , Offset
                     , Log
                    }
                    , {read_hunk
                       , Pid
                       , SeqNum
                       , Offset
                       , LenHint
                       , Log
                      }
                   ])
         end
        );
command_typegen(S,advance_seqnum) ->
    %% advance_seqnum(server(), incr())
    {advance_seqnum, command_typegen(S,server), command_typegen(S,incr)};
command_typegen(S,move_seq_to_longterm) ->
    %% move_seq_to_longterm(server(), seqnum())
    SeqNums = all_good_seqnums(S),
    ?LET({#server{pid=Pid},Log,SeqNum},oneof(SeqNums),
         {move_seq_to_longterm, Pid, SeqNum, Log});
command_typegen(S,get_current_seqnum) ->
    %% get_current_seqnum(server())
    {get_current_seqnum, command_typegen(S,server)};
command_typegen(S,get_all_seqnums) ->
    %% get_all_seqnums(server())
    {get_all_seqnums, command_typegen(S,server)};
command_typegen(S,get_current_seqnum_and_file_position) ->
    %% get_current_seqnum_and_file_position(server())
    {get_current_seqnum_and_file_position, command_typegen(S,server)};
command_typegen(S,get_current_seqnum_and_offset) ->
    %% get_current_seqnum_and_offset(server())
    {get_current_seqnum_and_offset, command_typegen(S,server)};
command_typegen(S,fold) ->
    %% fold(shortterm | longterm, dirname(), foldfun(), foldacc())
    {fold, oneof([shortterm, longterm]), command_typegen(S,dirname), fun foldfun/3, []};
command_typegen(_S,servername) ->
    oneof(?SERVERNAMES);
command_typegen(S,server) ->
    oneof(S#state.pids);
command_typegen(_S,bytes) ->
    nat();
command_typegen(_S,checkmd5) ->
    bool();
command_typegen(_S,dirname) ->
    oneof(?DIRNAMES);
command_typegen(_S,incr) ->
    int();
command_typegen(_S,lenhint) ->
    int();
command_typegen(_S,lenhintORxformfun) ->
    command_typegen(_S,lenhint); %% TODO: xformfun
command_typegen(_S,nth) ->
    nat();
command_typegen(_S,offset) ->
    int();
command_typegen(_S,props) ->
    ?LET(Server,command_typegen(_S,servername),
         ?LET(Bytes,command_typegen(_S,bytes),
              [{name,Server}
               , {file_len_max,1+((Bytes+1)*?FILELENFACTOR)}
               , {file_len_min,1+((Bytes+1)*?FILELENFACTOR)}
               , {long_h1_size,int()}
               , {long_h2_size,int()}]));
command_typegen(_S,seqnum) ->
    %% ASSUMPTION: No caller inside the brick_ets, gmt_hlog, and
    %% friends uses {0,0} as a seqnum+offset.  {0,0} has the same
    %% meaning as <<>>
    ?LET(I, int(), if I =:= 0 -> 1; true -> I end);
command_typegen(_S,xformfun) ->
    exit(not_implemented). %% TODO: xformfun

binary(I,J) ->
    ?LET(N,choose(I,J),binary(N)).

-spec initial_state() -> #state{}.
initial_state() ->
    initial_state([]).

-spec initial_state(proplist()) -> #state{}.
initial_state(Opts) ->
    #state{parallel=proplists:get_value(parallel, Opts, false)}.

%% %% state_is_sane(S::symbolic_state()) -> bool()
%% state_is_sane(_S) ->
%%     true.

%% next_state(S::symbolic_state(),R::var(),C::call()) -> symbolic_state()
%% restart
next_state(S,V,{call,_,restart,[Props]}) ->
    Key = proplists:get_value(name, Props),
    case is_restart_ok(S,Key) of
        true ->
            Servers = lists:keydelete(Key,#server.key,S#state.servers),
            S#state{pids=[V|S#state.pids], servers=[#server{pid=V,key=Key,props=Props}|Servers]};
        false ->
            S
    end;
%% stop
next_state(S,_V,{call,_,stop,[Pid]}) ->
    case is_stop_ok(S,Pid) of
        true ->
            Servers = [ X || #server{pid=Pid1}=X <- S#state.servers, Pid =/= Pid1 ],
            S#state{pids=S#state.pids -- [Pid], servers=Servers};
        false ->
            S
    end;
%% kill
next_state(S,_V,{call,_,kill,[Pid]}) ->
    S#state{pids=S#state.pids -- [Pid]
            , killed=[Pid|S#state.killed]
            , servers=[ X || #server{pid=Pid1}=X <- S#state.servers, Pid =/= Pid1 ]
           };
%% write_hunk
next_state(S,V,{call,_,write_hunk,[Pid,_LocalBrickName,HLogType,_Key,TypeNum,CBlobs,UBlobs]}) ->
    case is_alive(S,Pid) of
        true ->
            %% new Hunk
            SeqNum = seqnum(V),
            Offset = offset(V),
            {Len,_RawHunk} = create_hunk(TypeNum,CBlobs,UBlobs),
            Hunk = #hunk{type=HLogType,typenum=TypeNum,cblobs=CBlobs,ublobs=UBlobs,len=Len,seqnum=SeqNum,offset=Offset},
            case is_write_hunk_ok(S,Pid,Hunk) of
                true ->
                    %% fetch server
                    {value,Server,Servers} = lists:keytake(Pid, #server.pid, S#state.servers),
                    %% update log and servers
                    case is_write_shortterm_hunk(HLogType) of
                        true ->
                            Log = Server#server.short,
                            Hunks = Log#log.hunks,
                            S#state{servers=[Server#server{short=Log#log{hunks=[Hunk|Hunks]}}|Servers]};
                        false ->
                            Log = Server#server.long,
                            Hunks = Log#log.hunks,
                            S#state{servers=[Server#server{long=Log#log{hunks=[Hunk|Hunks]}}|Servers]}
                    end;
                {hunk_too_big,_} ->
                    S
            end;
        false ->
            S
    end;
%% read_hunk
next_state(S,V,{call,M,read_hunk,[Pid,SeqNum0,Offset0,Log]}) ->
    SeqNum = seqnum(SeqNum0),
    Offset = offset(Offset0),
    next_state_read_hunk(S,V,{call,M,read_hunk,[Pid,SeqNum,Offset,Log]});
next_state(S,V,{call,M,read_hunk,[Pid,SeqNum0,Offset0,_LenHint,Log]}) ->
    SeqNum = seqnum(SeqNum0),
    Offset = offset(Offset0),
    next_state_read_hunk(S,V,{call,M,read_hunk,[Pid,SeqNum,Offset,Log]});
%% advance_seqnum
next_state(S,_V,{call,_,advance_seqnum,[_Pid,_Incr]}) ->
    S; %% NOTE: we cannot care about the side-effects
%% move_seq_to_longterm
next_state(S,_V,{call,_,move_seq_to_longterm,[_Pid,_SeqNum,_Log]}) ->
    S; %% NOTE: we cannot care about the side-effects
%% get_all_seqnums
next_state(S,_V,{call,_,get_all_seqnums,[_Pid]}) ->
    S;
%% get_current_seqnum
next_state(S,_V,{call,_,get_current_seqnum,[_Pid]}) ->
    S;
%% get_current_seqnum_and_file_position
next_state(S,_V,{call,_,get_current_seqnum_and_file_position,[_Pid]}) ->
    S;
%% get_current_seqnum_and_offset
next_state(S,_V,{call,_,get_current_seqnum_and_offset,[_Pid]}) ->
    S;
%% fold
next_state(S,_V,{call,_,fold,[_Type,_Dirname,_Fun,_Acc]}) ->
    S.

next_state_read_hunk(S,V,{call,_,read_hunk,[Pid,SeqNum,_Offset,Log]}) ->
    if SeqNum =:= 0 ->
            S#state{pids=S#state.pids -- [Pid]
                    , killed=[Pid|S#state.killed]
                    , servers=[ X || #server{pid=Pid1}=X <- S#state.servers, Pid =/= Pid1 ]
                   };
       true ->
            case is_alive(S,Pid) of
                true ->
                    update_log_state(S,V,Pid,Log);
                false ->
                    S
            end
    end.

-spec invariant(#state{}) -> boolean().
invariant(_S) ->
    true.

%% precondition(S::symbolic_state(),C::call()) -> bool()
precondition(_S,_C) ->
    true.

%% postcondition(S::dynamic_state(),C::call(),R::term()) -> bool()
%% restart
postcondition(S,{call,_,restart,[Props]},Res) ->
    Key = proplists:get_value(name, Props),
    case Res of
        {ok,_Pid}=Res ->
            is_restart_ok(S,Key,Res);
        _ ->
            false
    end;
%% stop
postcondition(S,{call,_,stop,[Pid]},Res) ->
    case Res of
        ok ->
            is_stop_ok(S,Pid);
        {'EXIT', {noproc, _}} ->
            not is_stop_ok(S,Pid)
    end;
%% kill
postcondition(_S,{call,_,kill,_},_Res) ->
    true;
%% write_hunk
postcondition(S,{call,_,write_hunk,[Pid,_LocalBrickName,HLogType,_Key,TypeNum,CBlobs,UBlobs]},Res) ->
    {Len,_RawHunk} = create_hunk(TypeNum,CBlobs,UBlobs),
    Hunk = #hunk{type=HLogType,typenum=TypeNum,cblobs=CBlobs,ublobs=UBlobs,len=Len},
    case Res of
        {ok,SeqNum,Offset} when is_integer(SeqNum), is_integer(Offset) ->
            SeqNum =/=0 andalso Offset =/= 0 andalso is_alive(S,Pid) andalso is_write_hunk_ok(S,Pid,Hunk);
        {hunk_too_big, HLen} ->
            is_alive(S,Pid) andalso {hunk_too_big, HLen} =:= is_write_hunk_ok(S,Pid,Hunk);
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% read_hunk
postcondition(S,{call,M,read_hunk,[Pid,SeqNum0,Offset0,Log]},Res) ->
    SeqNum = seqnum(SeqNum0),
    Offset = offset(Offset0),
    postcondition_read_hunk(S,{call,M,read_hunk,[Pid,SeqNum,Offset,Log]},Res);
postcondition(S,{call,M,read_hunk,[Pid,SeqNum0,Offset0,_LenHint,Log]},Res) ->
    SeqNum = seqnum(SeqNum0),
    Offset = offset(Offset0),
    postcondition_read_hunk(S,{call,M,read_hunk,[Pid,SeqNum,Offset,Log]},Res);
%% advance_seqnum
postcondition(S,{call,_,advance_seqnum,[Pid,Incr]},Res) ->
    case Res of
        {ok,ResIncr} when is_integer(ResIncr) ->
            is_alive(S,Pid) andalso ResIncr =/= 0;
        {'EXIT', {badarg, _}} ->
            Incr =:= 0;
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% move_seq_to_longterm
postcondition(S,{call,_,move_seq_to_longterm,[Pid,SeqNum0,_Log]},[Res,_]) ->
    SeqNum = seqnum(SeqNum0),
    case Res of
        ok ->
            is_alive(S,Pid); %% NOTE: best we can do
        error ->
            is_alive(S,Pid); %% NOTE: best we can do
        {error,ResSeqNum} when is_integer(ResSeqNum) ->
            is_alive(S,Pid) andalso ResSeqNum > 0; %% NOTE: best we can do
        {'EXIT', {badarg, _}} ->
            SeqNum =< 0;
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid);
        badgen ->
            %% NOTE: This check is necessary to work around the method
            %% of implementating seqnum+offset for the quickcheck
            %% test.
            case SeqNum of
                {hunk_too_big,_} ->
                    io:format("g"),
                    is_alive(S,Pid);
                _ ->
                    false
            end
    end;
%% get_all_seqnums
postcondition(S,{call,_,get_all_seqnums,[Pid]},Res) ->
    case Res of
        Res when is_list(Res) ->
            %% TODO (SEQUENTIAL ONLY) - check contents of Res against state
            is_alive(S,Pid) andalso lists:all(fun(X) -> is_integer(X) andalso X =/= 0 end, Res);
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% get_current_seqnum
postcondition(S,{call,_,get_current_seqnum,[Pid]},Res) ->
    case Res of
        Res when is_integer(Res) ->
            %% TODO (SEQUENTIAL ONLY) - check contents of Res against state
            is_alive(S,Pid);
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% get_current_seqnum_and_file_position
postcondition(S,{call,_,get_current_seqnum_and_file_position,[Pid]},Res) ->
    case Res of
        {SeqNum,Offset} when is_integer(SeqNum), is_integer(Offset) ->
            %% TODO (SEQUENTIAL ONLY) - check contents of Res against state
            is_alive(S,Pid);
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% get_current_seqnum_and_offset
postcondition(S,{call,_,get_current_seqnum_and_offset,[Pid]},Res) ->
    case Res of
        {SeqNum,Offset} when is_integer(SeqNum), is_integer(Offset) ->
            %% TODO (SEQUENTIAL ONLY) - check contents of Res against state
            is_alive(S,Pid);
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end;
%% fold
postcondition(_S,{call,_,fold,[_Type,_Dirname,_Fun,_Acc]},Res) ->
    case Res of
        {Ok,[]} when is_list(Ok) ->
            %%io:format("fold: ~p ~p ~p ~p -> ~p~n", [_Type,_Dirname,_Fun,_Acc,Ok]),
            %% TODO (SEQUENTIAL ONLY) - check contents of Res against state
            lists:sort(Ok) =:= lists:usort(Ok)
    end;
postcondition(_S,_C,_R) ->
    false.

postcondition_read_hunk(S,{call,_,read_hunk,[Pid,SeqNum,Offset,_Log0]},[Res,_]) ->
    case Res of
        {ok,Binary} when is_binary(Binary) ->
            is_alive(S,Pid) andalso is_read_hunk_ok(S,Pid,SeqNum,Offset,Binary);
        {error,bad_hunk_header,Binary} when is_binary(Binary) ->
            is_alive(S,Pid) andalso not is_read_hunk_ok(S,Pid,SeqNum,Offset,Binary);
        {error,einval} when Offset =< 0 ->
            is_alive(S,Pid);
        {error,enoent} ->
            is_alive(S,Pid) andalso is_read_hunk_ok(S,Pid,SeqNum,Offset,undefined);
        eof ->
            is_alive(S,Pid) andalso is_read_hunk_ok(S,Pid,SeqNum,Offset,undefined);
        {'EXIT', {function_clause, [{gmt_hlog,log_file_path,_}|_]}} ->
            is_alive(S,Pid) andalso SeqNum =:= 0;
        badgen ->
            %% NOTE: This check is necessary to work around the method
            %% of implementating seqnum+offset for the quickcheck
            %% test.
            case {SeqNum, Offset} of
                {{hunk_too_big,_},{hunk_too_big,_}} ->
                    io:format("g"),
                    is_alive(S,Pid);
                _ ->
                    false
            end;
        {'EXIT', {noproc, _}} ->
            not is_alive(S,Pid)
    end.

-spec aggregate([{integer(), term(), term(), #state{}}])
               -> [{atom(), integer(), term()}].
aggregate(L) ->
    [ {Cmd,length(Args),filter_reply(Reply)} || {_N,{set,_,{call,_,Cmd,Args}},Reply,_State} <- L ].

filter_reply({'EXIT',{Err,_}}) ->
    {error,Err};
filter_reply(_) ->
    ok.

%% @doc setup helper
commands_setup(_Hard) ->
    %% start
    application:start(sasl),
    application:start(crypto),
    application:start(gmt),

    %% This will initialize app env 'brick_default_data_dir'
    application:load(gdss_brick),

    %% cleanup - regnames
    [ catch kill(Pid) || Pid <- ?REGNAMES ],
    timer:sleep(10),
    %% cleanup - rm -rf
    [ os:cmd("rm -rf " ++ X) || X <- ?DIRNAMES ],
    %% reset
    {ok,[]} = gmt_otp:reload_config(),
    {ok,noop}.

-spec stop(#state{}, #state{}) -> ok.
stop(_State0, undefined) ->
    ok;
stop(_State0, #state{pids=Pids}) ->
    %% cleanup - pids
    [ catch kill(Pid) || Pid <- Pids ],
    ok.


%%%----------------------------------------------------------------------
%%% IMPLEMENTATION
%%%----------------------------------------------------------------------

%% restart
restart(Props) ->
    Key = ?API_MOD:log_name2reg_name(proplists:get_value(name, Props)),
    Pid = spawn(),
    do(Pid, fun() ->
                    process_flag(trap_exit, true),
                    timer:sleep(10),
                    case erlang:whereis(Key) of
                        undefined ->
                            case catch ?API_MOD:start_link(Props) of
                                {ok, _Pid1}=Ok ->
                                    Ok;
                                _ ->
                                    restart(Props)
                            end;
                        Pid1 ->
                            {ok, Pid1}
                    end
            end).

%% stop
stop({ok,Pid}) ->
    stop(Pid);
stop(Pid) ->
    catch ?API_MOD:stop(Pid).

%% kill
kill(undefined) ->
    true;
kill({ok,Pid}) ->
    kill(Pid);
kill({error,_}) ->
    false; %% TBD
kill(Pid) when is_pid(Pid) ->
    Res = (catch exit(Pid,kill)),
    timer:sleep(10),
    Res;
kill(Name) when is_atom(Name) ->
    kill(whereis(Name)).

%% create_hunk
create_hunk(TypeNum,CBlobs,UBlobs) ->
    {Len,Hunk} = ?API_MOD:create_hunk(TypeNum,CBlobs,UBlobs),
    %% help simplify the test model by converting iolist() to binary()
    {Len,list_to_binary(Hunk)}.

%% write_hunk
write_hunk({ok,Pid},LocalBrickName,HLogType,Key,TypeNum,CBlobs,UBlobs) ->
    write_hunk(Pid,LocalBrickName,HLogType,Key,TypeNum,CBlobs,UBlobs);
write_hunk(Pid,LocalBrickName,HLogType,Key,TypeNum,CBlobs,UBlobs) ->
    catch ?API_MOD:write_hunk(Pid,LocalBrickName,HLogType,Key,TypeNum,CBlobs,UBlobs).

%% read_hunk
read_hunk({ok,Pid},SeqNum,Offset,Log) ->
    NewLog = Log#log{seqnum=logseqnum(Log),offset=logoffset(Log)},
    read_hunk(Pid,seqnum(SeqNum),offset(Offset),NewLog);
read_hunk(_Pid,{hunk_too_big,_},{hunk_too_big,_},Log) ->
    [badgen,Log];
read_hunk(Pid,SeqNum,Offset,Log) ->
    NewLog = sync_helper(Pid,SeqNum,Offset,Log),
    [?API_MOD:read_hunk(Pid,SeqNum,Offset), NewLog].

read_hunk({ok,Pid},SeqNum,Offset,LenHint,Log) ->
    NewLog = Log#log{seqnum=logseqnum(Log),offset=logoffset(Log)},
    read_hunk(Pid,seqnum(SeqNum),offset(Offset),LenHint,NewLog);
read_hunk(_Pid,{hunk_too_big,_},{hunk_too_big,_},_LenHint,Log) ->
    [badgen,Log];
read_hunk(Pid,SeqNum,Offset,LenHint,Log) ->
    NewLog = sync_helper(Pid,SeqNum,Offset,Log),
    [?API_MOD:read_hunk(Pid,SeqNum,Offset,LenHint), NewLog].

%% advance_seqnum
advance_seqnum({ok,Pid},Incr) ->
    advance_seqnum(Pid,Incr);
advance_seqnum(Pid,Incr) ->
    catch ?API_MOD:advance_seqnum(Pid,Incr).

%% move_seq_to_longterm
move_seq_to_longterm({ok,Pid},SeqNum,Log) ->
    NewLog = Log#log{seqnum=logseqnum(Log),offset=logoffset(Log)},
    move_seq_to_longterm(Pid,seqnum(SeqNum),NewLog);
move_seq_to_longterm(_Pid,{hunk_too_big,_},Log) ->
    [badgen,Log];
move_seq_to_longterm(Pid,SeqNum,Log) ->
    NewLog = sync_helper(Pid,SeqNum,undefined,Log),
    [catch ?API_MOD:move_seq_to_longterm(Pid,SeqNum),NewLog].

%% get_all_seqnums
get_all_seqnums({ok,Pid}) ->
    get_all_seqnums(Pid);
get_all_seqnums(Pid) ->
    catch ?API_MOD:get_all_seqnums(Pid).

%% get_current_seqnum
get_current_seqnum({ok,Pid}) ->
    get_current_seqnum(Pid);
get_current_seqnum(Pid) ->
    catch ?API_MOD:get_current_seqnum(Pid).

%% get_current_seqnum_and_file_position
get_current_seqnum_and_file_position({ok,Pid}) ->
    get_current_seqnum_and_file_position(Pid);
get_current_seqnum_and_file_position(Pid) ->
    catch ?API_MOD:get_current_seqnum_and_file_position(Pid).

%% get_current_seqnum_and_offset
get_current_seqnum_and_offset({ok,Pid}) ->
    get_current_seqnum_and_offset(Pid);
get_current_seqnum_and_offset(Pid) ->
    catch ?API_MOD:get_current_seqnum_and_offset(Pid).

%% fold
fold(ShortLong, Dir, Fun, Acc) ->
    catch ?API_MOD:fold(ShortLong, Dir, Fun, Acc).

%%%----------------------------------------------------------------------
%%% INTERNAL
%%%----------------------------------------------------------------------

%% sync (helper)
sync_helper(Pid,SeqNum,Offset,#log{type=short,seqnum=LogSeqNum,offset=LogOffset}=Log) ->
    %% ASSUMPTION: Caller must ensure a shortterm write has been
    %% synced (to disk) before a corresponding shortterm read.
    if LogSeqNum < SeqNum andalso (Offset =:= undefined orelse LogOffset < Offset) ->
            case catch ?API_MOD:sync(Pid,shortterm) of
                {ok, NewLogSeqNum, NewLogOffset} ->
                    io:format("s"),
                    Log#log{seqnum=NewLogSeqNum,offset=NewLogOffset};
                _ ->
                    Log
            end;
       true ->
            Log
    end;
sync_helper(Pid,SeqNum,Offset,#log{type=long,seqnum=LogSeqNum,offset=LogOffset}=Log) ->
    %% ASSUMPTION: This **quickcheck test** must ensure a longterm
    %% write has been synced (to disk) before a corresponding longterm
    %% read.
    if LogSeqNum < SeqNum andalso (Offset =:= undefined orelse LogOffset < Offset) ->
            case catch ?API_MOD:sync(Pid,longterm) of
                {ok, NewLogSeqNum, NewLogOffset} ->
                    io:format("S"),
                    Log#log{seqnum=NewLogSeqNum,offset=NewLogOffset};
                _ ->
                    Log
            end;
       true ->
            Log
    end.

foldfun(HunkSumm, _FH, Acc) ->
    [HunkSumm|Acc].

%% helpers
is_restart_ok(S,Key) ->
    [] =:= [ Pid1 || #server{pid=Pid1,key=Key1} <- S#state.servers, Key=:=Key1 ].

is_restart_ok(S,Key,Pid) ->
    case [ Pid1 || #server{pid=Pid1,key=Key1} <- S#state.servers, Key=:=Key1 ] of
        [] ->
            true;
        [Pid] ->
            true;
        _  ->
            false
    end.

is_stop_ok(S,Pid) ->
    [] =/= [ Pid1 || #server{pid=Pid1} <- S#state.servers, Pid=:=Pid1 ]
        andalso lists:member(Pid,S#state.pids).

is_alive(S,Pid) ->
    is_stop_ok(S,Pid).

is_write_hunk_ok(S,Pid,#hunk{len=Len}) ->
    [Props] = [ P || #server{pid=Pid1,props=P} <- S#state.servers, Pid=:=Pid1 ],
    FileLenMax = proplists:get_value(file_len_max,Props),
    %% ASSUMPTION: file_len_max is larger than 32
    if Len > FileLenMax - (32) -> % fudge factor!
            {hunk_too_big, Len};
       true ->
            true
    end.

is_write_shortterm_hunk(metadata) ->
    true;
is_write_shortterm_hunk(bigblob) ->
    true;
is_write_shortterm_hunk(bigblob_longterm) ->
    false.

is_read_hunk_ok(S,Pid,SeqNum,Offset,undefined) ->
    case [ Server || #server{pid=Pid1}=Server <- S#state.servers, Pid=:=Pid1 ] of
        [] ->
            true;
        [#server{short=#log{hunks=Hunks1},long=#log{hunks=Hunks2}}] ->
            Hunks = Hunks1++Hunks2,
            case [ Hunk || #hunk{seqnum=SeqNum1,offset=Offset1}=Hunk
                               <- Hunks, SeqNum=:=SeqNum1, Offset=:=Offset1 ] of
                [] ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end;
is_read_hunk_ok(S,Pid,SeqNum,Offset,Binary) ->
    case [ Server || #server{pid=Pid1}=Server <- S#state.servers, Pid=:=Pid1 ] of
        [] ->
            true;
        [#server{short=#log{hunks=Hunks1},long=#log{hunks=Hunks2}}] ->
            Hunks = Hunks1++Hunks2,
            case [ Hunk || #hunk{seqnum=SeqNum1,offset=Offset1}=Hunk
                               <- Hunks, SeqNum=:=SeqNum1, Offset=:=Offset1 ] of
                [#hunk{cblobs=CBlobs,ublobs=UBlobs}] ->
                    if CBlobs =/= [] ->
                            Check = Binary =:= hd(CBlobs),
                            if Check ->
                                    Check;
                               true ->
                                    io:format("cblobs: ~p ~p ~p => ~p =:= ~p",
                                              [Pid,SeqNum,Offset,Binary,CBlobs]),
                                    false
                            end;
                       UBlobs =/= [] ->
                            Check = Binary =:= hd(UBlobs),
                            if Check ->
                                    Check;
                               true ->
                                    io:format("ublobs: ~p ~p ~p => ~p =:= ~p",
                                              [Pid,SeqNum,Offset,Binary,UBlobs]),
                                    false
                            end;
                       true ->
                            false
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

all_good_hunks(#state{servers=Servers}) ->
    lists:flatten([[[ {Server,Log,Hunk}
                      || Hunk <- Log#log.hunks, is_good_hunk(Hunk) ]
                    || Log <- [Server#server.short,Server#server.long] ]
                   || Server <- Servers ]).

all_good_seqnums(#state{servers=Servers}) ->
    lists:usort(lists:flatten([[[ {Server,Log,Hunk#hunk.seqnum}
                                  || Hunk <- Log#log.hunks, is_good_hunk(Hunk) ]
                                || Log <- [Server#server.short,Server#server.long] ]
                               || Server <- Servers ])).

update_log_state(S,V,Pid,Log) ->
    LogSeqNum = logseqnum(V),
    LogOffset = logoffset(V),
    %% fetch server
    {value,Server,Servers} = lists:keytake(Pid, #server.pid, S#state.servers),
    %% update log and servers
    case Log of
        #log{type=short} ->
            NewLog = (Server#server.short)#log{seqnum=LogSeqNum,offset=LogOffset},
            S#state{servers=[Server#server{short=NewLog}|Servers]};
        #log{type=long} ->
            NewLog = (Server#server.long)#log{seqnum=LogSeqNum,offset=LogOffset},
            S#state{servers=[Server#server{long=NewLog}|Servers]}
    end.

is_good_hunk(#hunk{seqnum=SeqNum}) ->
    case seqnum(SeqNum) of
        {hunk_too_big,_} ->
            false;
        _ ->
            true
    end.

seqnum([Res,_Log]) ->
    Res;
seqnum({ok,SeqNum,_Offset}) ->
    SeqNum;
seqnum(SeqNum) ->
    SeqNum.

offset([Res,_Log]) ->
    Res;
offset({ok,_SeqNum,Offset}) ->
    Offset;
offset(Offset) ->
    Offset.

logseqnum([_Res,Log]) ->
    logseqnum(Log);
logseqnum(#log{seqnum=SeqNum}) ->
    seqnum(SeqNum);
logseqnum(SeqNum) ->
    SeqNum.

logoffset([_Res,Log]) ->
    logoffset(Log);
logoffset(#log{offset=Offset}) ->
    offset(Offset);
logoffset(Offset) ->
    Offset.

%% spawn
spawn() ->
    spawn(fun() -> loop() end).

loop() ->
    receive
        {From, Ref, F} ->
            case catch From ! {Ref, catch F()} of
                {'EXIT', Reason} ->
                    {error, Reason};
                _ ->
                    loop()
            end
    after 5000 ->
            {'EXIT', timeout}
    end.

do(Pid, F) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {self(), Ref, F},
    receive
        {'DOWN', Ref, process, Pid, Reason} ->
            {'EXIT', Reason};
        {Ref, Result} ->
            erlang:demonitor(Ref),
            Result
    after 5000 ->
            {'EXIT', timeout}
    end.

-endif. %% -ifdef(QC).
