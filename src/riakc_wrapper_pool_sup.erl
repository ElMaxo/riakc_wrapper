-module(riakc_wrapper_pool_sup).
-author('Max Davidenko').

-behaviour(supervisor).

%% API
-export([start_link/1, createChildSpecs/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  PoolSpecsCreator = fun({Name, SizeArgs, WorkerArgs}) ->
    PoolArgs = [{name, {local, Name}}, {worker_module, riakc_wrapper_worker}] ++ SizeArgs,
    poolboy:child_spec(Name, PoolArgs, WorkerArgs)
  end,
  PoolSpecs = lists:map(PoolSpecsCreator, Args),

  % Supervisor details
  RestartStrategy = one_for_one,
  MaxRestarts = 10,
  MaxSecondsBetweenRestarts = 10,
  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  {ok, {SupFlags, PoolSpecs}}.

createChildSpecs(Args) ->
  PoolSpecsCreator = fun({Name, SizeArgs, WorkerArgs}) ->
    PoolArgs = [{name, {local, Name}}, {worker_module, riakc_wrapper_worker}] ++ SizeArgs,
    poolboy:child_spec(Name, PoolArgs, WorkerArgs)
  end,
  [Specs | _] = lists:map(PoolSpecsCreator, Args),
  Specs.

%%%===================================================================
%%% Internal functions
%%%===================================================================
