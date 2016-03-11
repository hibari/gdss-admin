%%%----------------------------------------------------------------------
%%% Copyright (c) 2009-2016 Hibari developers.  All rights reserved.
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
%%% File     : brick_admin_cinfo.erl
%%% Purpose  : Cluster info/postmortem callback: GDSS config, stats, etc.
%%%----------------------------------------------------------------------

-module(brick_admin_cinfo).

-include("brick_admin.hrl").

%% Registration API
-export([register/0]).

%% Mandatory callbacks.
-export([cluster_info_init/0, cluster_info_generator_funs/0]).

register() ->
    cluster_info:register_app(?MODULE).

cluster_info_init() ->
    ok.

cluster_info_generator_funs() ->
    [
     {"GDSS: Admin Server status", fun admin_server_status/1},
     {"GDSS: Bootstrap brick config", fun bootstrap_config/1},
     {"GDSS: Admin Server schema", fun admin_server_schema/1},
     {"GDSS: Admin Status top", fun admin_status_top/1},
     {"GDSS: Admin Status client monitors", fun admin_status_client_mons/1},
     {"GDSS: History dump", fun history_dump/1}
    ].

admin_server_schema(C) ->
    Schema = (catch brick_admin:get_schema()),
    cluster_info:format(C, " Schema brick list:\n ~p\n\n",
                        [Schema#schema_r.schema_bricklist]),
    cluster_info:format(C, " Table definitions:\n ~p\n\n",
                        [dict:to_list(Schema#schema_r.tabdefs)]),
    cluster_info:format(C, " Chain to table mapping:\n ~p\n\n",
                        [dict:to_list(Schema#schema_r.chain2tab)]).

admin_server_status(C) ->
    cluster_info:format(C, " My node: ~p\n", [node()]),
    cluster_info:format(C, " Admin Server node: ~p\n",
                        [catch node(global:whereis_name(brick_admin))]),
    {ok, Distrib} = application:get_env(kernel, distributed),
    case lists:keyfind(gdss_admin, 1, Distrib) of
        {gdss_admin, _, Nodes} ->
            Nodes;
        {gdss_admin, Nodes} ->
            Nodes;
        false ->
            Nodes = []
    end,
    cluster_info:format(C, " Admin Server eligible nodes: ~p\n", [Nodes]).

admin_status_top(C) ->
    admin_http_to_text(C, "/").

admin_status_client_mons(C) ->
    admin_http_to_text(C, "/change_client_monitor.html").

bootstrap_config(C) ->
    {ok, Bin} = file:read_file(brick_admin:schema_filename()),
    cluster_info:send(C, Bin).

history_dump(C) ->
    Tmp = lists:flatten(io_lib:format("/tmp/history.~p",
                                      [gmt_time_otp18:system_time(micro_seconds)])),
    Res = try
              ok = mod_admin:dump_history(Tmp),
              {ok, Out} = file:read_file(Tmp),
              Out
          catch X:Y ->
                  io_lib:format("Error ~p ~p at ~p\n",
                                [X, Y, erlang:get_stacktrace()])
          after
              ok = file:delete(Tmp)
          end,
    cluster_info:send(C, Res).

%%%%%%%%%%

admin_http_to_text(C, UriSuffix) ->
    URL = "http://localhost:" ++
        gmt_util:list_ify(mod_admin:admin_server_http_port()) ++
        UriSuffix,
    Prog = case os:cmd("which lynx") of
               [$/|_] ->
                   "lynx -dump -width 130 ";
               _ ->
                   case os:cmd("which elinks") of
                       [$/|_] ->
                           "elinks -dump -dump-width 130 ";
                       _ ->
                           "echo Not available: "
                   end
           end,
    cluster_info:send(C, os:cmd(Prog ++ URL)).



