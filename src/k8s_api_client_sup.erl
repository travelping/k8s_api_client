-module(k8s_api_client_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

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
	#{strategy => one_for_all,
	  intensity => 0,
	  period => 1
    },
    ChildSpec =
	#{id => k8s_api_client_watch_sup,
	  start => {k8s_api_client_watch_sup, start_link, []},
	  type => supervisor,
	  restart => permanent,
	  shutdown => 5000
	 },
    {ok, {SupFlags, [ChildSpec]}}.
