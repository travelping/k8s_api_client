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

-define(IPPROTO_TCP,		6).

-define(TCP_KEEPIDLE,		4).	%% Start keeplives after this period
-define(TCP_KEEPINTVL,		5).	%% Interval between keepalives
-define(TCP_KEEPCNT,		6).	%% Number of keepalives before death

tcp_opts() ->
    tcp_opts(os:type(), [{keepalive, true}]).

tcp_opts({_, linux}, Opts) ->
    KeepIdle = 75,
    KeepIntvl = 75,

    [{raw, ?IPPROTO_TCP, ?TCP_KEEPIDLE, <<KeepIdle:32/native>>},
     {raw, ?IPPROTO_TCP, ?TCP_KEEPINTVL, <<KeepIntvl:32/native>>}
     | Opts ].

tls_opts(#{scheme := "http"}, _, Opts) ->
    Opts#{transport => tcp, tcp_opts => tcp_opts()};
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
	  tcp_opts => tcp_opts(),
	  tls_opts => maps:to_list(TLSopts)
	 }.

authority(#{host := Host, port := Port}, Opts) ->
    Transport = maps:get(transport, Opts, gun:default_transport(Port)),
    gun_http:host_header(Transport, Host, Port).
