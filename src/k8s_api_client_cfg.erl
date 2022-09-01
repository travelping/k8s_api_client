%% Copyright 2022, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(k8s_api_client_cfg).

-compile({no_auto_import,[get/0, get/1]}).
-compile({parse_transform, cut}).
-compile({parse_transform, do}).

-export([get/0, get_simple/0, init/0, init/1]).

-include_lib("kernel/include/logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================

get() ->
    case persistent_term:get(?MODULE, undefined) of
	undefined ->
	    {ok, Config} = init(),
	    persistent_term:put(?MODULE, Config),
	    Config;
	Cfg ->
	    Cfg
    end.

get_simple() ->
    maps:without([cert, key, cacerts], get()).

init() ->
    case init(pod) of
	{error, enoent} ->
	    init(kubeconfig);
	Other ->
	    Other
    end.

init(kubeconfig) ->
    do([error_m ||
	   File <- kubeconfig_path(),
	   Bin <- file:read_file(File),
	   Cfg <- parse_yaml(Bin),
	   set_config(Cfg)
       ]);
init(pod) ->
    BasePath = "/var/run/secrets/kubernetes.io/serviceaccount",
    do([error_m ||
	   CA <- file:read_file(filename:join(BasePath, "ca.crt")),
	   NameSpace <- file:read_file(filename:join(BasePath, "namespace")),
	   Token <- file:read_file(filename:join(BasePath, "token")),
	   return(
	     #{token => Token,
	       cacerts => [CA],
	       server => parse_uri(<<"https://kubernetes.default.svc">>),
	       namespace => NameSpace})
       ]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

kubeconfig_path() ->
    case os:getenv("KUBECONFIG") of
	false ->
	    {ok, filename_join_home(".kube/config")};
	File when is_list(File) ->
	    {ok, File}
    end.

%% from stdlib filename.erl
filename_join_home(File) ->
    case os:getenv("HOME") of
	false ->
	    {ok,[[Home]]} = init:get_argument(home),
	    filename:join(Home, File);
	Home  -> filename:join(Home, File)
    end.

parse_yaml(Bin) ->
    case fast_yaml:decode(Bin, [{maps, true}, sane_scalars]) of
	{ok, [Terms]} ->
	    {ok, Terms};
	Other ->
	    Other
    end.

set_config(#{<<"apiVersion">> := <<"v1">>, <<"kind">> := <<"Config">>} = Cfg) ->
    set_config_ctx(maps:get(<<"current-context">>, Cfg, undefined), Cfg);
set_config(_) ->
    {error, unsupport_config}.

set_config_ctx(Ctx, Cfg0) when is_binary(Ctx) ->
    Cfg = translate_cfg(Cfg0),
    {ok, #{<<"cluster">> := ClusterId, <<"user">> := UserId} = Context} =
	cfg_get_key([<<"contexts">>, Ctx], Cfg),
    {ok, User} = cfg_get_key([<<"users">>, UserId], Cfg),
    Config0 =
	case User of
	    #{<<"client-certificate-data">> := ClientCert,
	      <<"client-key-data">> := ClientKey} ->
		#{cert => decode_cert(ClientCert),
		  key => decode_key(ClientKey)};
	    #{<<"token">> := Token} ->
		#{token => Token}
	end,
    {ok, Cluster} = cfg_get_key([<<"clusters">>, ClusterId], Cfg),
    Config1 =
	case Cluster of
	    #{<<"certificate-authority-data">> := CA} ->
		Config0#{cacerts => decode_cacerts(CA)};
	    _  ->
		Config0
	end,
    Config =
	Config1#{server => parse_uri(maps:get(<<"server">>, Cluster)),
		 namespace => maps:get(<<"namespace">>, Context, undefined)},
    {ok, Config}.

cfg_get_key([], Cfg) ->
    {ok, Cfg};
cfg_get_key([H|T], Cfg) ->
    case is_map_key(H, Cfg) of
	true ->
	    cfg_get_key(T, maps:get(H, Cfg));
	_ ->
	    {error, {not_found, H}}
    end.

decode_key(Base64) ->
    Bin = base64:decode(Base64),
    [{Type, DER, not_encrypted}] = public_key:pem_decode(Bin),
    {Type, DER}.

decode_cert(Base64) ->
    Bin = base64:decode(Base64),
    [{'Certificate', DER, not_encrypted}] = public_key:pem_decode(Bin),
    DER.

decode_cacerts(Base64) ->
    Bin = base64:decode(Base64),
    [DER || {'Certificate', DER, not_encrypted} <- public_key:pem_decode(Bin)].

translate_cfg_map(Type, NameField, Items) when is_list(Items) ->
    lists:foldl(
      fun(#{Type := Entry, NameField := Name}, M) ->
	      M#{Name => Entry};
	 (_, M) ->
	      M
      end, #{}, Items);
translate_cfg_map(_, _, Items) ->
    Items.

translate_cfg_map({Key, Type, NameField}, Cfg)
  when is_map_key(Key, Cfg) ->
    maps:update_with(Key, translate_cfg_map(Type, NameField, _), Cfg);
translate_cfg_map(_, Cfg) ->
    Cfg.

translate_cfg(Cfg) ->
    CfgMaps =
	[{<<"clusters">>, <<"cluster">>, <<"name">>},
	 {<<"contexts">>, <<"context">>, <<"name">>},
	 {<<"users">>, <<"user">>, <<"name">>}],
    lists:foldl(fun translate_cfg_map/2, Cfg, CfgMaps).

parse_uri(URI) when is_list(URI) ->
    case uri_string:parse(URI) of
	#{host := _, path := _, scheme := "http"} = ParsedUri ->
	    maps:merge(#{port => 80}, ParsedUri);
	#{host := _, path := _, scheme := "https"} = ParsedUri ->
	    maps:merge(#{port => 443}, ParsedUri);
	_ ->
	    {error, uri_parse}
    end;
parse_uri(URI) when is_binary(URI) ->
    parse_uri(binary_to_list(URI));
parse_uri(_) ->
    {error, uri_parse}.
