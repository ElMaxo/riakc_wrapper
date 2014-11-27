-module(riakc_wrapper_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-author('Max Davidenko').

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(RECONNECTS, 60).
-define(TIMEOUT, 1000).

-record(state, {riak_kv_pid = null, use_objects_encoding = false, start_args = null, reconnects_left = ?RECONNECTS }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

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
init(Args) ->
% 1. Connect to Riak node
  Server = proplists:get_value(server, Args),
  Port = proplists:get_value(port, Args),
  UseEncoding = proplists:get_value(use_objects_encoding, Args),
  {ok, Pid} = riakc_pb_socket:start(Server, Port),
  {ok, #state{riak_kv_pid = Pid, use_objects_encoding = UseEncoding, start_args = Args }}.

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

% Callback for createObject api function
handle_call({create, Bucket, Key, Object}, _From, State) ->
  if
    State#state.use_objects_encoding == true ->
      RiakObject = encodeObject(Bucket, Key, Object);
    State#state.use_objects_encoding == false ->
      RiakObject = riakc_obj:new(Bucket, Key, Object)
  end,
  Reply = riakc_pb_socket:put(State#state.riak_kv_pid, RiakObject),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:put(NewState#state.riak_kv_pid, RiakObject),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for createLocalObject api function
handle_call({create_local, Bucket, Key, Object}, _From, State) ->
  if
    State#state.use_objects_encoding == true ->
      RiakObject = encodeObject(Bucket, Key, Object);
    State#state.use_objects_encoding == false ->
      RiakObject = riakc_obj:new(Bucket, Key, Object)
  end,
  {reply, RiakObject, State};

handle_call({create_local, Bucket, Key, Object, ContentType}, _From, State) ->
  RiakObject = riakc_obj:new(Bucket, Key, Object, ContentType),
  {reply, RiakObject, State};

% Callback for storeLocalObject api function
handle_call({store_local, RiakObject}, _From, State) ->
  Reply = riakc_pb_socket:put(State#state.riak_kv_pid, RiakObject),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:put(NewState#state.riak_kv_pid, RiakObject),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for storeLocalObject api function
handle_call({store_local, RiakObject, Options}, _From, State) ->
  Reply = riakc_pb_socket:put(State#state.riak_kv_pid, RiakObject, Options),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:put(NewState#state.riak_kv_pid, RiakObject, Options),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for getObject api function
handle_call({get, Bucket, Key}, _From, State) ->
  GetResult = riakc_pb_socket:get(State#state.riak_kv_pid, Bucket, Key),
  case GetResult of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:get(NewState#state.riak_kv_pid, Bucket, Key),
          case ReplyAfterReconnect of
            {ok, Fetched} ->
              ValCount = riakc_wrapper:getValuesCount(Fetched),
              if
                ValCount > 1 ->
                  Result = {reply, {error, sibling_detected, Fetched}, NewState};
                true ->
                  Result = {reply, decodeObject(Fetched), NewState}
              end;
            _ ->
              Result = {reply, ReplyAfterReconnect, NewState}
          end;
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      case GetResult of
        {ok, Fetched} ->
          ValCount = riakc_wrapper:getValuesCount(Fetched),
          if
            ValCount > 1 ->
              Result = {reply, {error, sibling_detected, Fetched}, State};
            true ->
              Result = {reply, decodeObject(Fetched), State}
          end;
        _ ->
          Result = {reply, GetResult, State}
      end
  end,
  Result;

% Callback for getTypedObjVal api function
handle_call({get_typed_obj, Type, Bucket, Key}, _From, State) ->
  GetResult = riakc_pb_socket:fetch_type(State#state.riak_kv_pid, {Bucket, Bucket}, Key),
  Result = case GetResult of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:fetch_type(NewState#state.riak_kv_pid, {Bucket, Bucket}, Key),
          case ReplyAfterReconnect of
            {ok, Obj} ->
              {reply, Type:value(Obj), NewState};
            _ ->
              {reply, ReplyAfterReconnect, NewState}
          end;
        _ ->
          {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      case GetResult of
        {ok, Obj} ->
          {reply, Type:value(Obj), State};
        _ ->
          {reply, GetResult, State}
      end
  end,
  Result;

% Callback for getRawTypedObj api function
handle_call({get_raw_typed_obj, Bucket, Key}, _From, State) ->
  GetResult = riakc_pb_socket:fetch_type(State#state.riak_kv_pid, {Bucket, Bucket}, Key),
  Result = case GetResult of
             {error, disconnected} ->
               RestoreResult = restoreConnection(State),
               case RestoreResult of
                 {noreply, NewState} ->
                   ReplyAfterReconnect = riakc_pb_socket:fetch_type(NewState#state.riak_kv_pid, {Bucket, Bucket}, Key),
                   case ReplyAfterReconnect of
                     {ok, Obj} ->
                       {reply, Obj, NewState};
                     _ ->
                       {reply, ReplyAfterReconnect, NewState}
                   end;
                 _ ->
                   {stop, connection_lost, {error, disconnected}, State}
               end;
             _ ->
               case GetResult of
                 {ok, Obj} ->
                   {reply, Obj, State};
                 _ ->
                   {reply, GetResult, State}
               end
           end,
  Result;

% Callback for storeTypedObject api function
handle_call({store_typed_obj, Bucket, Key, Type, Obj}, _From, State) ->
  Reply = riakc_pb_socket:update_type(State#state.riak_kv_pid, {Bucket, Bucket}, Key, Type:to_op(Obj)),
  Result = case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:update_type(NewState#state.riak_kv_pid, {Bucket, Bucket}, Key, Type:to_op(Obj)),
          {reply, ReplyAfterReconnect, NewState};
        _ ->
          {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      {reply, Reply, State}
  end,
  Result;

% Callback for getRawObject api function
handle_call({get_raw, Bucket, Key}, _From, State) ->
  GetResult = riakc_pb_socket:get(State#state.riak_kv_pid, Bucket, Key),
  case GetResult of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:get(NewState#state.riak_kv_pid, Bucket, Key),
          case ReplyAfterReconnect of
            {ok, Fetched} ->
              ValCount = riakc_wrapper:getValuesCount(Fetched),
              if
                ValCount > 1 ->
                  Result = {reply, {error, sibling_detected, Fetched}, NewState};
                true ->
                  Result = {reply, {ok, Fetched}, NewState}
              end;
            _ ->
              Result = {reply, ReplyAfterReconnect, NewState}
          end;
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      case GetResult of
        {ok, Fetched} ->
          ValCount = riakc_wrapper:getValuesCount(Fetched),
          if
            ValCount > 1 ->
              Result = {reply, {error, sibling_detected, Fetched}, State};
            true ->
              Result = {reply, {ok, Fetched}, State}
          end;
        _ ->
          Result = {reply, GetResult, State}
      end
  end,
  Result;

% Callback for getRawObject api function
handle_call({get_raw, Bucket, Key, Options}, _From, State) ->
  GetResult = riakc_pb_socket:get(State#state.riak_kv_pid, Bucket, Key, Options),
  case GetResult of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:get(NewState#state.riak_kv_pid, Bucket, Key, Options),
          case ReplyAfterReconnect of
            {ok, Fetched} ->
              ValCount = riakc_wrapper:getValuesCount(Fetched),
              if
                ValCount > 1 ->
                  Result = {reply, {error, sibling_detected, Fetched}, NewState};
                true ->
                  Result = {reply, {ok, Fetched}, NewState}
              end;
            _ ->
              Result = {reply, ReplyAfterReconnect, NewState}
          end;
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      case GetResult of
        {ok, Fetched} ->
          ValCount = riakc_wrapper:getValuesCount(Fetched),
          if
            ValCount > 1 ->
              Result = {reply, {error, sibling_detected, Fetched}, State};
            true ->
              Result = {reply, {ok, Fetched}, State}
          end;
        _ ->
          Result = {reply, GetResult, State}
      end
  end,
  Result;

% Callback for updateObject api function
handle_call({update, Bucket, Key, NewValue}, _From, State) ->
  GetResult = riakc_pb_socket:get(State#state.riak_kv_pid, Bucket, Key),
  case GetResult of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:get(NewState#state.riak_kv_pid, Bucket, Key),
          case ReplyAfterReconnect of
            {ok, OldObject} ->
              if
                NewState#state.use_objects_encoding == true ->
                  EncodedData = term_to_binary(NewValue, [compressed]),
                  NewObject = riakc_obj:update_value(OldObject, EncodedData);
                NewState#state.use_objects_encoding == false ->
                  NewObject = riakc_obj:update_value(OldObject, NewValue)
              end,
              Result = {reply, riakc_pb_socket:put(NewState#state.riak_kv_pid, NewObject), NewState};
            _ ->
              Result = {reply, ReplyAfterReconnect, NewState}
          end;
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      case GetResult of
        {ok, OldObject} ->
          if
            State#state.use_objects_encoding == true ->
              EncodedData = term_to_binary(NewValue, [compressed]),
              NewObject = riakc_obj:update_value(OldObject, EncodedData);
            State#state.use_objects_encoding == false ->
              NewObject = riakc_obj:update_value(OldObject, NewValue)
          end,
          Result = {reply, riakc_pb_socket:put(State#state.riak_kv_pid, NewObject), State};
        {error, _Reason} ->
          Result = {reply, GetResult, State}
      end
  end,
  Result;

% Callback for updateObject api function
handle_call({full_update, RiakObject}, _From, State) ->
  Reply = riakc_pb_socket:put(State#state.riak_kv_pid, RiakObject),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:put(NewState#state.riak_kv_pid, RiakObject),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for deleteObject api function
handle_call({delete, Bucket, Key}, _From, State) ->
  Reply = riakc_pb_socket:delete(State#state.riak_kv_pid, Bucket, Key),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:delete(NewState#state.riak_kv_pid, Bucket, Key),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for searchBySecondaryIndex api function
handle_call({query_by_index, Bucket, Index, IndexKey}, _From, State) ->
  Reply = riakc_pb_socket:get_index_eq(State#state.riak_kv_pid, Bucket, Index, IndexKey),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:get_index_eq(NewState#state.riak_kv_pid, Bucket, Index, IndexKey),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for setBucketProperties api function
handle_call({bucket_props, Bucket, NVal, AllowMult}, _From, State) ->
  Reply = riakc_pb_socket:set_bucket(State#state.riak_kv_pid, Bucket, [{n_val, NVal}, {allow_mult, AllowMult}]),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:set_bucket(NewState#state.riak_kv_pid, Bucket, [{n_val, NVal}, {allow_mult, AllowMult}]),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for getKeysList api function
handle_call({keys_list, Bucket}, _From, State) ->
  Reply = riakc_pb_socket:list_keys(State#state.riak_kv_pid, Bucket),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:list_keys(NewState#state.riak_kv_pid, Bucket),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for getBucketsList api function
handle_call(list_buckets, _From, State) ->
  Reply = riakc_pb_socket:list_buckets(State#state.riak_kv_pid),
  case Reply of
    {error, disconnected} ->
      RestoreResult = restoreConnection(State),
      case RestoreResult of
        {noreply, NewState} ->
          ReplyAfterReconnect = riakc_pb_socket:list_buckets(NewState#state.riak_kv_pid),
          Result = {reply, ReplyAfterReconnect, NewState};
        _ ->
          Result = {stop, connection_lost, {error, disconnected}, State}
      end;
    _ ->
      Result = {reply, Reply, State}
  end,
  Result;

% Callback for encodeData api function
handle_call({encode_data, Data}, _From, State) ->
  if
    State#state.use_objects_encoding == true ->
      EncodedData = term_to_binary(Data, [compressed]);
    true ->
      EncodedData = Data
  end,
  Reply = EncodedData,
  {reply, Reply, State};

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

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
terminate(_Reason, State) ->
  % Disconnecting from server
  riakc_pb_socket:stop(State#state.riak_kv_pid),
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

encodeObject(Bucket, Key, Object) ->
  EncodedObject= term_to_binary(Object, [compressed]),
  riakc_obj:new(Bucket, Key, EncodedObject, <<"application/x-erlang-term">>).

decodeObject(RiakObject) ->
  case riakc_obj:get_content_type(RiakObject) of
    "application/x-erlang-term" ->
      safeBinToTerm(riakc_obj:get_value(RiakObject));
    "application/x-erlang-binary" ->
      safeBinToTerm(riakc_obj:get_value(RiakObject));
    _ ->
      {ok, riakc_obj:get_value(RiakObject)}
  end.

safeBinToTerm(Bin) ->
  try
    binary_to_term(Bin)
  catch
    _:Reason ->
      {error, Reason}
  end.

restoreConnection(State)  when State#state.reconnects_left =< 0 ->
  {stop, no_reconnects_left, State};
restoreConnection(State) ->
  Server = proplists:get_value(server, State#state.start_args),
  Port = proplists:get_value(port, State#state.start_args),
  ReconnectResult = riakc_pb_socket:start(Server, Port),
  case ReconnectResult of
    {ok, Pid} ->
      NewState = State#state{ riak_kv_pid = Pid, reconnects_left = ?RECONNECTS },
      Result = {noreply, NewState};
    _ ->
      AttemptsLeft = State#state.reconnects_left,
      NewState = State#state{reconnects_left = AttemptsLeft - 1},
      timer:sleep(?TIMEOUT),
      Result = restoreConnection(NewState)
  end,
  Result.