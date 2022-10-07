%% Copyright 2022, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(k8s_api_client_watch).

-compile({parse_transform, cut}).

-behavior(gen_statem).

%% API
-export([start_link/1, all/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3, code_change/4]).

-include_lib("kernel/include/logger.hrl").

-define(TIMEOUT, 5000).
-define(DEBUG_OPTS, [{install, {fun logger_sys_debug:logger_gen_statem_trace/3, ?MODULE}}]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Watch) ->
    SpawnOpts = [{fullsweep_after, 0}],
    proc_lib:start_link(?MODULE, init, [[self(), Watch]], infinity, SpawnOpts).

all({_, TID}) ->
    ets:tab2list(TID).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() -> [handle_event_function, state_enter].

init([Parent, Watch]) ->
    process_flag(trap_exit, true),

    TID = ets:new(?MODULE, [set, public, {keypos, 1}]),

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
    Headers = k8s_api_client_resource:headers(Config),

    ?LOG(debug, "Path: ~p~nHeaders: ~p~n", [Path, Headers]),
    {async, {ConnPid, StreamRef}} =
	k8s_api_client_connection:get(Path, Headers),
    monitor(process, ConnPid),

    %% gen_statem init return does not allow extra terms
    proc_lib:init_ack(Parent, {ok, self(), handle(Data)}),

    LoopOpts = [{debug, ?DEBUG_OPTS}],
    gen_statem:enter_loop(
      ?MODULE, LoopOpts, {loading, init}, Data#{conn := ConnPid, stream := StreamRef}).

handle_event(enter, _, {watch, init}, #{pending := Pending})
  when Pending =/= <<>> ->
    ?LOG(error, "initial load incomplete: ~p", [Pending]),
    {stop, {error, incomplete}};

handle_event(enter, _, {watch, init}, #{config := Config, watch := Watch} = Data) ->
    Path = query(Watch, true, Config, Data),
    Headers = k8s_api_client_resource:headers(Config),

    ?LOG(debug, "Path: ~p~nHeaders: ~p~n", [Path, Headers]),
    {async, {ConnPid, StreamRef}} =
	k8s_api_client_connection:get(Path, Headers),
    monitor(process, ConnPid),
    {keep_state, Data#{conn := ConnPid, stream := StreamRef}};

handle_event(enter, _, _, _) ->
    keep_state_and_data;

handle_event(info, {gun_response, ConnPid, StreamRef, nofin, 200, _Headers}, {Phase, init},
	     #{conn := ConnPid, stream := StreamRef} = Data) ->
    {next_state, {Phase, data}, Data};

handle_event(info, {gun_response, ConnPid, StreamRef, nofin, Status, _Headers}, {Phase, init},
	     #{watch := Watch, conn := ConnPid, stream := StreamRef} = Data)
  when Status >= 400 ->
    ?LOG(critical, "k8s API requests for ~p failed with HTTP Status code ~p",
	 [Watch, Status]),
    {next_state, {Phase, error}, Data};

handle_event(info, {gun_response, ConnPid, StreamRef, nofin, Status, _Headers}, {Phase, init},
	     #{watch := Watch, conn := ConnPid, stream := StreamRef} = Data) ->
    ?LOG(critical, "k8s API requests for ~p returned unexpected HTTP Status code ~p",
	 [Watch, Status]),
    {next_state, {Phase, error}, Data};

handle_event(info, {gun_response, ConnPid, _StreamRef, fin, 200, _Headers},
	     {Phase = loading, _} = State, #{conn := ConnPid} = Data0) ->
    Data = handle_api_data(State, <<>>, Data0),

    process_event_notify(Phase, done, Data),
    {next_state, {watch, init}, Data#{conn := undefined, stream := undefined}};

handle_event(info, {gun_response, ConnPid, StreamRef, fin, Status, _Headers}, _,
	     #{conn := ConnPid, stream := StreamRef}) ->
    ?LOG(debug, "~p: stream closed with status ~p", [ConnPid, Status]),
    {stop, normal};

handle_event(info, {gun_data, ConnPid, StreamRef, nofin, _Bin} = Msg, {_Phase, error} = State,
	     #{conn := ConnPid, stream := StreamRef} = _Data0) ->
    ?LOG(debug, "ErrorEvMsg: ~p, State: ~p", [Msg, State]),
    keep_state_and_data;

handle_event(info, {gun_data, ConnPid, StreamRef, fin, _Bin} = Msg, {_Phase, error} = State,
	     #{conn := ConnPid, stream := StreamRef} = _Data0) ->
    ?LOG(debug, "ErrorEvMsg: ~p, State: ~p", [Msg, State]),

    %% the normal supervisor restarts will deal with connection restarts, but here we
    %% are in a critical error state, kill the application to prevent supervisor restarts
    application:stop(k8s_api_client),
    {stop, normal};

handle_event(info, {gun_data, ConnPid, StreamRef, fin, Bin}, {Phase, data} = State,
	     #{conn := ConnPid, stream := StreamRef} = Data0) ->
    Data = handle_api_data(State, Bin, Data0),

    process_event_notify(Phase, done, Data),
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

handle_event(info, {'DOWN', _, process, ConnPid, Reason}, {watch, init}, #{conn := ConnPid}) ->
    ?LOG(info, "~p: watch connection failed with ~p", [ConnPid, Reason]),
    {stop, normal};

handle_event(info, {'DOWN', _, process, ConnPid, Reason}, _, #{conn := ConnPid} = Data) ->
    ?LOG(info, "~p: connection terminated with ~p", [ConnPid, Reason]),
    {next_state, {watch, init}, Data#{conn := undefined, stream := undefined}};

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

query(QuerySpec, DoWatch, Config, #{resourceVersion := ResourceVersion}) ->
    Parameter0 = maps:get(parameter, QuerySpec, #{}),
    Parameter1 = Parameter0#{resourceVersion => ResourceVersion},
    Parameter = case DoWatch of
		    true -> Parameter1#{watch => true};
		    _    -> Parameter1
		end,
    k8s_api_client_resource:query(QuerySpec#{parameter => Parameter}, Config).

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

process_event(Type, Resource, #{tid := TID} = Data) ->
    ?LOG(debug, "process_event, got ~p on ~p", [Type, Resource]),
    Old = case ets:lookup(TID, object_key(Resource)) of
	      [{_, Object}] -> Object;
	      _             -> undefined
	  end,
    case Type of
	delete ->
	    ets:delete(TID, object_key(Resource));
	_ ->
	    ets:insert(TID, {object_key(Resource), Resource})
    end,
    process_event_notify(Type, {Resource, Old}, Data),
    ok.

process_event_notify(Phase, done, _) when Phase =/= loading ->
    ok;
process_event_notify(Type, Resource, #{watch := #{callback := Cb}} = Data)
  when is_function(Cb, 3) ->
    Cb(handle(Data), Type, Resource);
process_event_notify(_, _, _) ->
    ok.

handle(#{tid := TID}) ->
    {self(), TID}.

atom(Bin) when is_binary(Bin) ->
    binary_to_atom(string:lowercase(Bin), latin1);
atom(Atom) when is_atom(Atom) ->
    Atom.

object_key(#{metadata := #{namespace := Namespace, name := Name}}) ->
    {Namespace, Name}.

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
