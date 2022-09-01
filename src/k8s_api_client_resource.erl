%% Copyright 2022, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(k8s_api_client_resource).

-export([query/2, query/3, headers/1, update/4]).

-include_lib("kernel/include/logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================

api(Root, Path, #{server := #{path := ServerRoot}}) ->
    lists:join($/, [ServerRoot, Root | Path]).

path(Root, false, Resource, Config) ->
    api(Root, [Resource], Config);
path(Root, true, Resource, #{namespace := Namespace} = Config) when is_binary(Namespace) ->
    api(Root, [<<"namespaces">>, Namespace, Resource], Config);
path(Root, Namespace, Resource, Config)
  when is_list(Namespace); is_binary(Namespace) ->
    api(Root, [<<"namespaces">>, Namespace, Resource], Config).

path(#{root := Root, resource := Resource} = Watch, API) ->
    iolist_to_binary(path(Root, maps:get(namespace, Watch, true), Resource, API)).

to_bin(B) when is_binary(B) ->
    B;
to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) when is_list(L) ->
    iolist_to_binary(L);
to_bin(I) when is_integer(I) ->
    integer_to_binary(I).

query_kv({K, V}) ->
    {to_bin(K), to_bin(V)}.

query(Query) when is_map(Query) ->
    Q = lists:map(fun query_kv/1, maps:to_list(Query)),
    uri_string:compose_query(Q).

query(#{resource := Resource} = ResourceSpec, API) ->
    Parameter = maps:get(parameter, ResourceSpec, #{}),
    Path = path(Resource, API),
    Query = query(Parameter),
    uri_string:recompose(#{path => Path, query => Query}).

query(#{resource := Resource} = ResourceSpec, Item, API) ->
    Parameter = maps:get(parameter, ResourceSpec, #{}),
    Path = path(Resource, API),
    Query = query(Parameter),
    uri_string:recompose(#{path => [Path, $/, Item], query => Query}).

headers() ->
    [{<<"accept">>, <<"application/json">>}].

headers(#{token := Token}) ->
    [{<<"authorization">>, iolist_to_binary(["Bearer ", Token])}
    | headers()];
headers(_) ->
    headers().

%%%===================================================================
%%% API
%%%===================================================================

update(Resource, Name, API, JSON) ->
    case get(Resource, Name, API) of
	{ok, {404, _}} ->
	    case post(Resource, #{}, API, jsx:encode(JSON, [{indent, 4}])) of
		{ok, {201, _}} ->
			ok;
		{ok, {Status, Body}} ->
		    ?LOG(error, "unexpected status ~p from GET: ~p", [Status, Body]),
		    {error, unexpected_status, Status};
		{error, _} = Error ->
		    Error
	    end;
	{ok, {200, #{metadata := #{resourceVersion := ResourceVersion}} = Body}} ->
	    Diff0 = json_diff(Body, JSON),
	    Diff = Diff0#{metadata => #{resourceVersion => ResourceVersion}},
	    case patch(Resource, Name, #{}, API, jsx:encode(Diff, [{indent, 4}])) of
		{ok, {200, _}} ->
		    ok;
		{ok, {Status, Body}} ->
		    ?LOG(error, "unexpected status ~p from PATCH: ~p", [Status, Body]),
		    {error, unexpected_status, Status};
		{error, _} = Error ->
		    Error
	    end;
	{error, _} = Error ->
	    Error
    end.

%%%=========================================================================
%%%  internal functions
%%%=========================================================================

get(Resource, Item, API) ->
    Query = k8s_api_client_resource:query(Resource, Item, API),
    Headers = k8s_api_client_resource:headers(API),
    io:format("GET Query: ~p~n", [Query]),

    {async, {ConnPid, StreamRef}} = k8s_api_client_connection:get(Query, Headers),
    case gun:await(ConnPid, StreamRef) of
	{response, nofin, Status, RespHeaders} ->
	    {ok, Body} = gun:await_body(ConnPid, StreamRef),
	    Data = handle_response_body(RespHeaders, Body),
	    {ok, {Status, Data}};

	{response, fin, Status, _RespHeaders} ->
	    {ok, {Status, <<>>}};

	_Other ->
	    io:format("Other:~n~p~n", [_Other]),
	    {error, failed}
    end.

post(Resource, Parameter, API, Body) ->
    Query = k8s_api_client_resource:query(Resource#{parameter => Parameter}, API),
    Headers = [{<<"content-type">>, <<"application/json">>}
	       | k8s_api_client_resource:headers(API)],
    io:format("POST Query: ~p~n", [Query]),

    {async, {ConnPid, StreamRef}} = k8s_api_client_connection:post(Query, Headers, Body),
    case gun:await(ConnPid, StreamRef) of
	{response, nofin, Status, RespHeaders} ->
	    {ok, RespBody} = gun:await_body(ConnPid, StreamRef),
	    Data = handle_response_body(RespHeaders, RespBody),
	    {ok, {Status, Data}};

	{response, fin, Status, _RespHeaders} ->
	    {ok, {Status, <<>>}};

	_Other ->
	    io:format("Other:~n~p~n", [_Other]),
	    {error, failed}
    end.

patch(Resource, Item, Parameter, API, Body) ->
    Query = k8s_api_client_resource:query(Resource#{parameter => Parameter}, Item, API),
    Headers = [{<<"content-type">>, <<"application/strategic-merge-patch+json">>}
	      | k8s_api_client_resource:headers(API)],
    io:format("PATCH Query: ~p~n", [Query]),

    {async, {ConnPid, StreamRef}} = k8s_api_client_connection:patch(Query, Headers, Body),
    case gun:await(ConnPid, StreamRef) of
	{response, nofin, Status, RespHeaders} ->
	    {ok, RespBody} = gun:await_body(ConnPid, StreamRef),
	    Data = handle_response_body(RespHeaders, RespBody),
	    {ok, {Status, Data}};

	{response, fin, Status, _RespHeaders} ->
	    {ok, {Status, <<>>}};

	_Other ->
	    io:format("PATCH Other:~n~p~n", [_Other]),
	    {error, failed}
    end.

handle_response_body(Headers, Body) ->
    case proplists:get_value(<<"content-type">>, Headers, undefined) of
	<<"application/json">> ->
	    jsx:decode(Body, [return_maps, {labels, attempt_atom}]);
	_ ->
	    Body
    end.

json_equal(V1, V2) when is_binary(V1), is_atom(V2) ->
    V1 =:= atom_to_binary(V2);
json_equal(V1, V2) when is_atom(V1), is_binary(V2) ->
    atom_to_binary(V1) =:= V2;
json_equal(V1, V2) ->
    V1 =:= V2.

json_diff(M1, M2) when is_map(M1), is_map(M2) ->
    Diff = maps:from_keys(maps:keys(M1) -- maps:keys(M2), null),
    maps:fold(
      fun(K, V, M) ->
	      Old = maps:get(K, M1),
	      case json_equal(Old, V) of
		  false ->
		      M#{K => json_diff(Old, V)};
		  _ ->
		      M
	      end
      end, Diff, M2);
json_diff(_, M2) ->
    M2.
