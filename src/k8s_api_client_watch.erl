%% Copyright 2022, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(k8s_api_client_watch).

-compile({parse_transform, cut}).

-behavior(gen_statem).

%% API
-export([start_link/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-include_lib("kernel/include/logger.hrl").

-define(TIMEOUT, 5000).
-define(DEBUG_OPTS, [{install, {fun logger_sys_debug:logger_gen_statem_trace/3, ?MODULE}}]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Watch) ->
    Opts = [{debug, ?DEBUG_OPTS}],
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [Watch], Opts).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() -> [handle_event_function, state_enter].

init([Watch]) ->
    process_flag(trap_exit, true),

    TID = ets:new(?SERVER, [set, public, {keypos, 1}]),

    Config = k8s_api_client_cfg:get_simple(),

    Data = #{tid => TID,
	     cycle => make_ref(),
	     config => Config,
	     watch => Watch,

	     resourceVersion => 0,
	     conn => undefined,
	     stream => undefined,
	     pending => <<>>
	    },

    Path = query(Watch, false, Config, Data),
    Headers = headers(Config),

    ?LOG(debug, "Path: ~p~nHeaders: ~p~n", [Path, Headers]),
    {async, {ConnPid, StreamRef}} =
	k8s_api_client_connection:get(Path, Headers),

    {ok, {loading, init}, Data#{conn := ConnPid, stream := StreamRef}}.

handle_event(enter, _, {watch, init}, #{pending := Pending})
  when Pending =/= <<>> ->
    ?LOG(error, "initial load incomplete: ~p", [Pending]),
    {stop, {error, incomplete}};

handle_event(enter, _, {watch, init}, #{config := Config, watch := Watch} = Data) ->
    Path = query(Watch, true, Config, Data),
    Headers = headers(Config),

    ?LOG(debug, "Path: ~p~nHeaders: ~p~n", [Path, Headers]),
    {async, {ConnPid, StreamRef}} =
	k8s_api_client_connection:get(Path, Headers),
    {keep_state, Data#{conn := ConnPid, stream := StreamRef}};

handle_event(enter, _, _, _) ->
    keep_state_and_data;

handle_event(info, {gun_response, ConnPid, StreamRef, nofin, 200, _Headers}, {Phase, init},
	     #{conn := ConnPid, stream := StreamRef} = Data) ->
    {next_state, {Phase, data}, Data};

handle_event(info, {gun_response, ConnPid, _StreamRef, fin, 200, _Headers}, {loading, _} = State,
	     #{conn := ConnPid} = Data0) ->
    Data = handle_api_data(State, <<>>, Data0),
    {next_state, {watch, init}, Data};

handle_event(info, {gun_response, ConnPid, StreamRef, fin, Status, _Headers}, _,
	     #{conn := ConnPid, stream := StreamRef}) ->
    ?LOG(debug, "~p: stream closed with status ~p", [ConnPid, Status]),
    {stop, normal};

handle_event(info, {gun_data, ConnPid, StreamRef, fin, Bin}, {_, data} = State,
	     #{conn := ConnPid, stream := StreamRef} = Data0) ->
    Data = handle_api_data(State, Bin, Data0),
    {next_state, {watch, init}, Data#{conn := undefined, stream := undefined}};

handle_event(info, {gun_data, ConnPid, StreamRef, nofin, Bin}, State,
	     #{conn := ConnPid, stream := StreamRef} = Data0) ->
    Data = handle_api_data(State, Bin, Data0),
    {keep_state, Data};

handle_event(info, {gun_down, ConnPid, _Protocol, State, _Streams}, _, #{conn := ConnPid}) ->
    ?LOG(debug, "~p: connection closed with ~p", [ConnPid, State]),
    {stop, normal};

%% handle_event(info, {gun_response, ConnPid, _StreamRef, fin, Status, _Headers} = _Msg, _State,
%%	     #{conn := ConnPid} = _Data) ->
%%     ?LOG(debug, "Info: ~p, State: ~p", [_Msg, _State]),
%%     ?LOG(info, "~p: got response ~p", [ConnPid, Reason]),
%%     keep_state_and_data;

handle_event(_Ev, _Msg, _State, _Data) ->
    ?LOG(debug, "Ev: ~p, Msg: ~p, State: ~p", [_Ev, _Msg, _State]),
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%=========================================================================
%%%  internal functions
%%%=========================================================================

api(Version, Path, #{server := #{path := Root}}) ->
    lists:join($/, [Root, <<"api">>, Version|Path]).

path(Version, false, Resource, Config) ->
    api(Version, [Resource], Config);
path(Version, true, Resource, #{namespace := Namespace} = Config) when is_binary(Namespace) ->
    api(Version, [<<"namespaces">>, Namespace, Resource], Config);
path(Version, Namespace, Resource, Config)
  when is_list(Namespace); is_binary(Namespace) ->
    api(Version, [<<"namespaces">>, Namespace, Resource], Config).

path(#{version := Version, resource := Resource} = Watch, Config) ->
    iolist_to_binary(path(Version, maps:get(namespace, Watch, true), Resource, Config)).

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

query(Query, true) when is_map(Query) ->
    query(Query#{watch => true});
query(Query, _) ->
    query(Query).

query(Query) when is_map(Query) ->
    Q = lists:map(fun query_kv/1, maps:to_list(Query)),
    uri_string:compose_query(Q).

query(#{resource := Resource} = Watch, DoWatch, Config,
      #{resourceVersion := ResourceVersion}) ->
    Parameter0 = maps:get(parameter, Watch, #{}),
    Parameter = Parameter0#{resourceVersion => ResourceVersion},

    Path = path(Resource, Config),
    Query = query(Parameter, DoWatch),
    uri_string:recompose(#{path => Path, query => Query}).

headers() ->
    [{<<"accept">>, <<"application/json">>}].

headers(#{token := Token}) ->
    [{<<"authorization">>, iolist_to_binary(["Bearer ", Token])}
     | headers()];
headers(_) ->
    headers().

handle_api_data(State, Bin, #{pending := In} = Data) ->
    handle_api_data(State, Data#{pending := <<In/binary, Bin/binary>>}).

handle_api_data(State, #{pending := In} = Data0) ->
    case binary:split(In, <<$\n>>) of
	[In] -> %% no newline,
	    Data0;
	[Head, Tail] ->
	    Data = process_api_data(State, Head, Data0),
	    handle_api_data(State, Data#{pending => Tail})
    end.

process_api_data(State, Bin, Data0) ->
    case jsx:decode(Bin, [{labels, attempt_atom}, return_maps]) of
	Object when is_map(Object) ->
	    process_api_object(State, Object, Data0);

	Ev ->
	    ?LOG(info, "unexpected message from k8s: ~p", [Ev]),
	    Data0
    end.

process_api_object({watch, _}, #{type := Type, object := Object} = Ev, Data) ->
    ?LOG(debug, "process_api_object, watch got event: ~p", [Ev]),
    process_event(atom(Type), Object, Data),
    set_resource_version(Object, Data);

process_api_object({loading, _}, #{items := Items, metadata := Meta} = Ev, Data) ->
    ?LOG(debug, "process_api_object, load got event: ~p", [Ev]),
    lists:foreach(process_event(init, _, Data), Items),
    set_resource_version(Meta, Data).

process_event(Type, Resource, #{tid := TID, watch := Watch}) ->
    ?LOG(debug, "process_event, got ~p on ~p", [Type, Resource]),
    case Type of
	delete ->
	    ets:delete(TID, object_uid(Resource));
	_ ->
	    ets:insert(TID, {object_uid(Resource), Resource})
    end,
    process_event_notify(Type, Resource, Watch),
    ok.

process_event_notify(Type, Resource, #{callback := Cb})
  when is_function(Cb, 2) ->
    Cb(Type, Resource);
process_event_notify(_, _, _) ->
    ok.

atom(Bin) when is_binary(Bin) ->
    binary_to_atom(string:lowercase(Bin), latin1);
atom(Atom) when is_atom(Atom) ->
    Atom.

object_uid(#{metadata := #{uid := UID}}) ->
    UID.

-if(0).

read_event(#{kind := Kind,
	     metadata :=
		 #{namespace := Namespace,
		   uid := PodUid,
		   name := PodName,
		   resourceVersion := ResourceVersion
		  } = Metadata,
	     status :=
		 #{phase := Phase} = Status}, Data) ->
    Ev =
	#{resource_version => binary_to_integer(ResourceVersion),
	  namespace => binary_to_list(Namespace),
	  pod_uid => binary_to_list(PodUid),
	  pod_name => binary_to_list(PodName),
	  pod_ip => binary_to_list(maps:get(podIP, Status, <<>>)),
	  nets_data => ?Tools:pod_nets_data(
			  maps:get(annotations, Metadata, #{}),
			  Data
			 ),
	  phase => binary_to_list(Phase),
	  agent => ?Tools:pod_container_state(
		      maps:get(agent_container_name, Data),
		      maps:get(containerStatuses, Status, [])
		     ),
	  init_agent => ?Tools:pod_container_state(
			   maps:get(agent_init_container_name, Data),
			   maps:get(initContainerStatuses, Status, [])
			  )
	 },
    %%?LOG(info, "ICS: ~p", [maps:get(initContainerStatuses, Status, [])]),
    {atom(Kind), Ev};

read_event(#{code := Code,
	     kind := Kind,
	     message := Message = <<"too old resource version:", Versions/binary>>,
	     reason := Reason,
	     status := Status
	    }, _Data) ->
    Ev =
	#{code => Code,
	  reason => binary_to_list(Reason),
	  status => binary_to_list(Status),
	  message => binary_to_list(Message),
	  resource_version =>
	      begin
		  [_DesiredVersion, OldestVersion] = string:lexemes(Versions, "( )"),
		  binary_to_integer(OldestVersion) - 1
	      end
	 },
    {atom(Kind), Ev}.

-endif.

set_resource_version(#{metadata := Meta}, Data) ->
    set_resource_version(Meta, Data);
set_resource_version(#{resourceVersion := Value}, #{resourceVersion := Old} = Data)
  when is_binary(Value) ->
    case binary_to_integer(Value) of
	New when New > Old ->
	    Data#{resourceVersion := New};
	_ ->
	    Data
    end.
