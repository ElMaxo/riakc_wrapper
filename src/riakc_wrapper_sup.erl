-module(riakc_wrapper_sup).
-author('Max Davidenko').
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, transient, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  ConnectionsManagerMonitor = ?CHILD(riakc_wrapper_poolmgr, worker),
  RestartStrategy = {one_for_one, 5, 60},
  Childs = [ConnectionsManagerMonitor],
  {ok, { RestartStrategy, Childs} }.