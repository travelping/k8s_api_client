%% Copyright 2022, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(k8s_api_client_connection).

-export([start_pool/0, await_up/0,
	 get/2, get/3,
	 patch/2, patch/3, patch/4,
	 post/2, post/3, post/4,
	 put/2, put/3, put/4]).

-include_lib("kernel/include/logger.hrl").

-define(HTTP_CONNECT_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

start_pool() ->
    case k8s_api_client_cfg:get() of
	#{server := #{host := Host, port := Port} = Server} = Config ->
	    ConnOpts0 = #{connect_timeout => ?HTTP_CONNECT_TIMEOUT,
			  protocols => [http2, http]},
	    ConnOpts = tls_opts(Server, Config, ConnOpts0),

	    %% gun_pool taking the transport key violated thes gun_pool:opts() type
	    %% specification. This look like a bug in gun_pool. Lets duplicate the
	    %% transport key into the pool opts just to be sure.
	    Opts = #{transport => maps:get(transport, ConnOpts),
		     conn_opts => ConnOpts},

	    {ok, ManagerPid} = gun_pool:start_pool(Host, Port, Opts),

	    try gun_pool:await_up(ManagerPid) of
		ok ->
		    Authority = authority(Server, Opts),
		    application:set_env(k8s_api_client, '$authority', Authority),
		    ok
	    catch
		exit:{timeout, _} ->
		    {error, no_connection}
	    end;
	{error, _} = Error ->
	    Error
    end.

await_up() ->
    case application:get_env(k8s_api_client, '$authority', undefined) of
	undefined -> {error, pool_not_started, 'No pool was started.'};
	Authority -> gun_pool:await_up(Authority)
    end.

get(Path, Headers) ->
    gun_pool:get(Path, headers(Headers)).

get(Path, Headers, ReqOpts) ->
    gun_pool:get(Path, headers(Headers), ReqOpts).

patch(Path, Headers) ->
    gun_pool:patch(Path, headers(Headers)).

patch(Path, Headers, ReqOpts) when is_map(ReqOpts) ->
    gun_pool:patch(Path, headers(Headers), ReqOpts);
patch(Path, Headers, Body) ->
    gun_pool:patch(Path, headers(Headers), Body).

patch(Path, Headers, Body, ReqOpts) ->
    gun_pool:patch(Path, headers(Headers), Body, ReqOpts).

post(Path, Headers) ->
    gun_pool:post(Path, headers(Headers)).

post(Path, Headers, ReqOpts) when is_map(ReqOpts) ->
    gun_pool:post(Path, headers(Headers), ReqOpts);
post(Path, Headers, Body) ->
    gun_pool:post(Path, headers(Headers), Body).

post(Path, Headers, Body, ReqOpts) ->
    gun_pool:post(Path, headers(Headers), Body, ReqOpts).

put(Path, Headers) ->
    gun_pool:put(Path, headers(Headers)).

put(Path, Headers, ReqOpts) when is_map(ReqOpts) ->
    gun_pool:put(Path, headers(Headers), ReqOpts);
put(Path, Headers, Body) ->
    gun_pool:put(Path, headers(Headers), Body).

put(Path, Headers, Body, ReqOpts) ->
    gun_pool:put(Path, headers(Headers), Body, ReqOpts).

headers(Headers) when is_map(Headers) ->
    {ok, Authority} = application:get_env(k8s_api_client, '$authority'),
    Headers#{<<"host">> => Authority};
headers(Headers) when is_list(Headers) ->
    {ok, Authority} = application:get_env(k8s_api_client, '$authority'),
    lists:keystore(<<"host">>, 1, Headers, {<<"host">>, Authority}).

%%%=========================================================================
%%%  internal functions
%%%=========================================================================

tls_opts(#{scheme := "http"}, _, Opts) ->
    Opts#{transport => tcp};
tls_opts(#{scheme := "https"}, Config, Opts) ->
    TLSopts0 = maps:with([cert, key, cacerts], Config),
    TLSopts1 = TLSopts0#{alpn_advertised_protocols => [<<"h2">>, <<"http/1.1">>]},
    VerifyType =
	case Config of
	    #{cacerts := [_]} -> verify_peer;
	    _ -> verify_none
	end,
    TLSopts = TLSopts1#{verify => VerifyType},
    Opts#{transport => tls,
	  tls_opts => maps:to_list(TLSopts)
	 }.

authority(#{host := Host, port := Port}, Opts) ->
    Transport = maps:get(transport, Opts, gun:default_transport(Port)),
    gun_http:host_header(Transport, Host, Port).

-if(0).

%% API
-export([start_link/0, await_up/0]).
%%-export([http_request/3, http_request/6]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-include_lib("kernel/include/logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================

-define(HTTP_STREAM_RECV_TIMEOUT, 60 * 1000).
-define(HTTP_CONNECT_TIMEOUT, 5 *1000).

start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

await_up() ->
    gen_statem:call(?SERVER, await_up).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() -> handle_event_function.

init(_) ->
    process_flag(trap_exit, true),

    {ok, Config} = k8s_api_client_cfg:get(),
    {ok, ConnPid} = gun_start_pool(Config),
    MRef = monitor(process, ConnPid),

    Data = #{config => Config, conn => ConnPid, mref => MRef},
    {ok, init, Data}.

handle_event(info, {'DOWN', MRef, process, ConnPid, Reason}, _,
	     #{mref := MRef, conn := ConnPid}) ->
    ?LOG(info, "~p: terminated with ~p", [ConnPid, Reason]),
    gun:close(ConnPid),
    {stop, normal};

handle_event(info, {gun_up, ConnPid, _Protocol}, init, #{conn := ConnPid} = Data) ->
    ?LOG(info, "Command Channel UP Protocol: ~p", [_Protocol]),
    {next_state, up, Data};
handle_event(info, {gun_error, ConnPid, Reason}, _, #{conn := ConnPid}) ->
    ?LOG(error, "Connection Error: ~p", [Reason]),
    gun:close(ConnPid),
    {stop, normal};
handle_event(info, {gun_down, _StreamPid, Protocol, Reason}, _, #{conn := ConnPid}) ->
    ?LOG(error, "Connection Down: ~p: ~p", [Protocol, Reason]),
    gun:close(ConnPid),
    {stop, normal};

handle_event({call, From}, await_up, up, #{conn := ConnPid}) ->
    {keep_state_and_data, [{reply, From, {ok, ConnPid}}]};
handle_event({call, _From}, await_up, _, _Data) ->
    {keep_state_and_data, [postpone]};

handle_event(_Ev, _Msg, _State, _Data) ->
    ?LOG(error, "Ev: ~p, Msg: ~p, State: ~p", [_Ev, _Msg, _State]),
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%=========================================================================
%%%  internal functions
%%%=========================================================================

tls_opts(#{scheme := "http"}, _, Opts) ->
    Opts#{transport => tcp};
tls_opts(#{scheme := "https"}, Config, Opts) ->
    TLSopts0 = maps:with([cert, key, cacerts], Config),
    TLSopts = TLSopts0#{verify => verify_peer,
			alpn_advertised_protocols => [<<"h2">>, <<"http/1.1">>]},
    Opts#{transport => tls,
	  tls_opts => maps:to_list(TLSopts)
	 }.

gun_open(#{server := #{host := Host, port := Port} = Server} = Config) ->
    Opts0 = #{connect_timeout => ?HTTP_CONNECT_TIMEOUT,
	      protocols => [http2, http]},
    Opts = tls_opts(Server, Config, Opts0),
    {ok, ConnPid} = gun:open(Host, Port, Opts),
    ?LOG(info, "~p: connecting to ~s", [ConnPid, uri_string:recompose(Server)]),
    {ok, ConnPid}.

http_request(Resource, Query, Config) ->
    http_request(get, Resource, Query, [], [], Config).

http_request(Method, Resource, Query, RequestHeaders, RequestBody,
	     #{token := Token}) ->

    {ok, ConnPid} = open(),

    QueryStr = uri_string:compose_query(Query),
    Path = uri_string:recompose(#{path => Resource, query => QueryStr}),
    RequestOpts = #{reply_to => self()},

    StreamRef = gun:request(ConnPid, method(Method), Path,
			    headers(Token, RequestHeaders),
			    RequestBody, RequestOpts),
    Response =
	case gun:await(ConnPid, StreamRef) of
	    {response, fin, Status, _Headers} ->
		{error, Status};
	    {response, nofin, 200, _Headers} ->
		{ok, Body} = gun:await_body(ConnPid, StreamRef),
		Doc = jsx:decode(Body, [return_maps, {labels, attempt_atom}]),
		{ok, Doc};
	    {response, nofin, Status, Headers} ->
		{ok, Body} = gun:await_body(ConnPid, StreamRef),
		{error, {Status, Headers, Body}}
	end,
    Response.

headers(Token, Headers) ->
    [{<<"authorization">>, iolist_to_binary(["Bearer ", Token])}|Headers].

method(Method) when is_atom(Method) ->
    list_to_binary(string:uppercase(atom_to_list(Method))).
-endif.
