-module(k8s_api_client_watch_sup).

-behaviour(supervisor).

-export([start_link/0, start_watch/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    k8s_api_client_connection:await_up(),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_watch(Watch) ->
    supervisor:start_child(?SERVER, [Watch]).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    %% this makes sure that we own the jesse schema db
    %% ergw_api_client:load_schemas(),

    SupFlags =
	#{strategy => simple_one_for_one,
	  intensity => 2,
	  period => 1000
    },
    ChildSpec =
	#{id => ergw_sbi_client_watch,
	  start => {k8s_api_client_watch, start_link, []},
	  type => worker,
	  restart => transient,
	  shutdown => 5000
	 },
    {ok, {SupFlags, [ChildSpec]}}.
