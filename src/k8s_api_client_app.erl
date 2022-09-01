-module(k8s_api_client_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    logger:set_primary_config(level, debug),
    k8s_api_client_sup:start_link().

stop(_State) ->
    ok.
