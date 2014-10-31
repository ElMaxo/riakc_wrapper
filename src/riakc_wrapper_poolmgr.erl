-module(riakc_wrapper_poolmgr).
-author('Max Davidenko').
-behaviour(gen_server).

%% API
-export([start_link/0, startPool/1, stopPool/1, stop/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {pools = null, pools_sup = null}).

%%%===================================================================
%%% API
%%%===================================================================

startPool(Args) ->
  gen_server:call(?SERVER, {start_pool, Args}, infinity).

stopPool(Pool) ->
  gen_server:call(?SERVER, {stop_pool, Pool}, infinity).

stop() ->
  gen_server:call(?SERVER, stop, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
  {ok, #state{pools = []}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call({start_pool, Args}, _From, State) ->
  [{PoolName, _, _} | _] = Args,
  PoolSup = State#state.pools_sup,
  if
    PoolSup == null ->
      {ok, SupPid} = riakc_wrapper_pool_sup:start_link(Args),
      NewState = State#state{ pools = State#state.pools ++ [PoolName], pools_sup = SupPid};
    true ->
      PoolSpecs = riakc_wrapper_pool_sup:createChildSpecs(Args),
      supervisor:start_child(PoolSup, PoolSpecs),
      NewState = State#state{ pools = State#state.pools ++ [PoolName]}
  end,
  {reply, {ok, "Riak connection pool successfuly started"}, NewState};

handle_call({stop_pool, PoolName}, _From, State) ->
  PoolTerminator = fun(Pool) ->
    if
      Pool == PoolName ->
        poolboy:stop(Pool),
        false;
      true ->
        true
    end
  end,
  NewState = State#state{ pools = lists:filter(PoolTerminator, State#state.pools) },
  {reply, {ok, "Riak connection pool successfuly stopped"}, NewState};

handle_call(stop, _From, State) ->
  PoolsTerminator = fun(Pool) ->
    poolboy:stop(Pool),
    Pool
  end,
  lists:map(PoolsTerminator, State#state.pools),
  Pid = whereis(riakc_wrapper_pool_sup),
  if
    Pid /= undefined ->
      exit(Pid, shutdown);
    true ->
      ok
  end,
  {stop, normal, "Workers pool manager stopped", State};

handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
