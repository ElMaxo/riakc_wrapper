-module(riakc_wrapper).
-behaviour(application).
-author('Max Davidenko').

%% Application callbacks
-export([start/0, start/2, stop/0, stop/1, startPool/1, stopPool/1]).

%% API
-export([storeData/4, storeData/5, storeData/6, createObject/4, createLocalObject/4, createLocalObject/5,
  storeLocalObject/2, storeLocalObject/3, getObject/3, getRawObject/3, getRawObject/4, updateObject/4, updateObject/2, deleteObject/3,
  setBucketProperties/4, getKeysList/2, getBucketsList/1, searchBySecondaryIndex/4,
  createEmptySet/0, addToSet/2, removeFromSet/2, getSetSize/1, setContainsValue/2, getTypedObjVal/4, getRawTypedObj/3, getLocalSetVal/1, storeTypedObject/5,
  createEmptyMap/0, storeAtMap/2, removeFromMap/2, fetchFromMap/2, fetchMapKeys/1, mapContainsKey/2, getMapSize/1]).

%% Helpers
-export([getValuesCount/1, getValues/1, getObjectSibling/2, getObjectMetadata/1, updateObjectMetadata/2, getMetadataEntry/2,
  getMetadataEntries/1, setMetadataEntry/3, deleteMetadataEntry/2, clearMetadata/1, addSecondaryIndex/2, setSecondaryIndex/2,
  getSecondaryIndex/2, getSecondaryIndexes/1, deleteSecondaryIndex/2, clearSecondaryIndexes/1, addLink/2, setLink/2,
  getLinks/2, getAllLinks/1, deleteLinks/2, clearLinks/1]).

-define(SERVER, ?MODULE).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
  IsRunning = whereis(riakc_wrapper_poolmgr),
  case IsRunning of
    undefined ->
      start(normal, []);
    _App ->
      ok
  end.

stop() ->
  IsRunning = whereis(riakc_wrapper_poolmgr),
  case IsRunning of
    undefined ->
      ok;
    _App ->
      stop([])
  end.

start(_StartType, _Args) ->
  %Args = [{riakpool, [{size, 3}, {max_overflow, 20}], [{server, "127.0.0.1"}, {port, 8087}, {use_objects_encoding, true}]}],
  riakc_wrapper_sup:start_link().

stop(_State) ->
  riakc_wrapper_poolmgr:stop(),
  Pid = whereis(riakc_wrapper_sup),
  if
    Pid /= undefined ->
      exit(Pid, shutdown);
    true ->
      ok
  end,
  ok.


%%%===================================================================
%%% API functions
%%%===================================================================

startPool(Args) ->
  riakc_wrapper_poolmgr:startPool(Args).

stopPool(Pool) ->
  riakc_wrapper_poolmgr:stopPool(Pool).

storeData(PoolName, Bucket, Key, Data) ->
  FindResult = getRawObject(PoolName, Bucket, Key),
  case FindResult of
    {error, notfound} ->
      createObject(PoolName, Bucket, Key, Data);
    {error, sibling_detected, BrokenObject} ->
      PartlyRepairedObject = riakc_obj:select_sibling(1, BrokenObject),
      PartlyRepairResult = updateObject(PoolName, PartlyRepairedObject),
      {error, sibling_detected, PartlyRepairResult};
    {ok, Object} ->
      EncodedData = encodeData(PoolName, Data),
      NewObject = riakc_obj:update_value(Object, EncodedData, <<"application/x-erlang-term">>),
      updateObject(PoolName, NewObject);
    UnknowResult ->
      {error, UnknowResult}
  end.

storeData(PoolName, Bucket, Key, Data, []) ->
  storeData(PoolName, Bucket, Key, Data);
storeData(PoolName, Bucket, Key, Data, Indexes) ->
  FindResult = getRawObject(PoolName, Bucket, Key),
  case FindResult of
    {error, notfound} ->
      NewObject = createLocalObject(PoolName, Bucket, Key, Data),
      MD = riakc_obj:get_update_metadata(NewObject),
      NewMD = riakc_obj:set_secondary_index(MD, Indexes),
      FinalObj = riakc_obj:update_metadata(NewObject, NewMD),
      storeLocalObject(PoolName, FinalObj);
    {error, sibling_detected, BrokenObject} ->
      PartlyRepairedObject = riakc_obj:select_sibling(1, BrokenObject),
      PartlyRepairResult = updateObject(PoolName, PartlyRepairedObject),
      {error, sibling_detected, PartlyRepairResult};
    {ok, Object} ->
      EncodedData = encodeData(PoolName, Data),
      NewObject = riakc_obj:update_value(Object, EncodedData, <<"application/x-erlang-term">>),
      MD = riakc_obj:get_update_metadata(NewObject),
      NewMD = riakc_obj:set_secondary_index(MD, Indexes),
      FinalObj = riakc_obj:update_metadata(NewObject, NewMD),
      updateObject(PoolName, FinalObj);
    UnknowResult ->
      {error, UnknowResult}
  end.

storeData(PoolName, Bucket, Key, Data, [], []) ->
  storeData(PoolName, Bucket, Key, Data);
storeData(PoolName, Bucket, Key, Data, Indexes, []) ->
  storeData(PoolName, Bucket, Key, Data, Indexes);
storeData(PoolName, Bucket, Key, Data, Indexes, ContentType) ->
  FindResult = getRawObject(PoolName, Bucket, Key),
  case FindResult of
    {error, notfound} ->
      NewObject = createLocalObject(PoolName, Bucket, Key, Data, ContentType),
      MD = riakc_obj:get_update_metadata(NewObject),
      NewMD = riakc_obj:set_secondary_index(MD, Indexes),
      FinalObj = riakc_obj:update_metadata(NewObject, NewMD),
      storeLocalObject(PoolName, FinalObj);
    {error, sibling_detected, BrokenObject} ->
      PartlyRepairedObject = riakc_obj:select_sibling(1, BrokenObject),
      PartlyRepairResult = updateObject(PoolName, PartlyRepairedObject),
      {error, sibling_detected, PartlyRepairResult};
    {ok, Object} ->
      NewObject = riakc_obj:update_value(Object, Data, ContentType),
      MD = riakc_obj:get_update_metadata(NewObject),
      NewMD = riakc_obj:set_secondary_index(MD, Indexes),
      FinalObj = riakc_obj:update_metadata(NewObject, NewMD),
      updateObject(PoolName, FinalObj);
    UnknowResult ->
      {error, UnknowResult}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Creates specified object at specified bucket
%%
%% @spec createObject(Bucket, Key, Object) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% Object - Object to create and store - term()
%% @end
%%--------------------------------------------------------------------
createObject(PoolName, Bucket, Key, Object) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {create, Bucket, Key, Object}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Creates specified object at specified bucket without storing it in riak
%%
%% @spec createLocalObject(Bucket, Key, Object) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% Object - Object to create and store - term()
%% @end
%%--------------------------------------------------------------------
createLocalObject(PoolName, Bucket, Key, Object) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {create_local, Bucket, Key, Object}, infinity)
  end, infinity).

createLocalObject(PoolName, Bucket, Key, Object, ContentType) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {create_local, Bucket, Key, Object, ContentType}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Stores specified riak object
%%
%% @spec storeLocalObject(RiakOject) -> ok | {error, Error}
%% Key - Key to associate object with - binary()
%% Object - Object to create and store - riakc_obj
%% @end
%%--------------------------------------------------------------------
storeLocalObject(PoolName, RiakObject) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {store_local, RiakObject}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Stores specified riak object
%%
%% @spec storeLocalObject(RiakOject, Options) -> ok | {error, Error}
%% Key - Key to associate object with - binary()
%% Object - Object to create and store - riakc_obj
%% Options - Store options - any()
%% @end
%%--------------------------------------------------------------------
storeLocalObject(PoolName, RiakObject, Options) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {store_local, RiakObject, Options}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get object associated with key at specified bucket
%%
%% @spec getObject(Bucket, Key) -> {ok, Object} | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% @end
%%--------------------------------------------------------------------
getObject(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get, Bucket, Key}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get object associated with key at raw riak format
%%
%% @spec getRawObject(Bucket, Key) -> {ok, Riak Object} | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% @end
%%--------------------------------------------------------------------
getRawObject(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get_raw, Bucket, Key}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get object associated with key at raw riak format
%%
%% @spec getRawObject(Bucket, Key, Options) -> {ok, Riak Object} | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% Options - get options - any()
%% @end
%%--------------------------------------------------------------------
getRawObject(PoolName, Bucket, Key, Options) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get_raw, Bucket, Key, Options}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Updates specified object at specified bucket
%%
%% @spec updateObject(Bucket, Key, NewValue) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% NewValue - New object value to create and store - term()
%% @end
%%--------------------------------------------------------------------
updateObject(PoolName, Bucket, Key, NewValue) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {update, Bucket, Key, NewValue}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Updates specified object at storage
%%
%% @spec updateObject(RiakObject) -> ok | {error, Error}
%% RiakObject - riakc_obj()
%% @end
%%--------------------------------------------------------------------
updateObject(PoolName, RiakObject) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {full_update, RiakObject}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Deletes object associated with key at specified bucket
%%
%% @spec deleteObject(Bucket, Key) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% @end
%%--------------------------------------------------------------------
deleteObject(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {delete, Bucket, Key}, infinity)
  end, infinity).

searchBySecondaryIndex(PoolName, Bucket, Index, IndexKey) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {query_by_index, Bucket, Index, IndexKey}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Sets specified bucket properties
%%
%% @spec setBucketProperties(Bucket, NVal, AllowMult) -> ok | {error, Error}
%% NVal - replicas count
%% AllowMult - multiple objects by one key storage permission
%% @end
%%--------------------------------------------------------------------
setBucketProperties(PoolName, Bucket, NVal, AllowMult) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {bucket_props, Bucket, NVal, AllowMult}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get keys list for specified bucket
%%
%% @spec getKeysList(Bucket) -> keys list [Key1, Key2...] | {error, Error}
%% Bucket - bucket to get keys for
%% @end
%%--------------------------------------------------------------------
getKeysList(PoolName, Bucket) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {keys_list, Bucket}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get buckets list at specified server
%%
%% @spec getBucketList() -> buckets list [Bucket1, Bucket2...] | {error, Error}
%% @end
%%--------------------------------------------------------------------
getBucketsList(PoolName) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, list_buckets, infinity)
  end, infinity).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get specified object count at bucket (if multiple storage allowed)
%%
%% @spec getValuesCount(RiakObject) -> val_count
%% RiakObject - riak object instance
%% @end
%%--------------------------------------------------------------------
getValuesCount(RiakObject) ->
  riakc_obj:value_count(RiakObject).

%%--------------------------------------------------------------------
%% @doc
%% Get values of specified object at bucket (if multiple storage allowed)
%%
%% @spec getValues(RiakObject) -> list of object siblings [Key:Value1, Key:Value2...]
%% RiakObject - riak object instance
%% @end
%%--------------------------------------------------------------------
getValues(RiakObject) ->
  CTypes = riakc_obj:get_content_types(RiakObject),
  [CType | _] = CTypes,
  Values = riakc_obj:get_values(RiakObject),
  Decoder = fun(Arg) ->
    if
      CType == "application/x-erlang-term" ->
        Result = binary_to_term(Arg);
      true ->
        Result = Arg
    end,
    Result
  end,
  lists:map(Decoder, Values).

%%--------------------------------------------------------------------
%% @doc
%% Get specified object sibling at specified position (if multiple storage allowed)
%%
%% @spec getObjectSibling(SiblingIndex, RiakObject) -> Riak object
%% SiblingIndex - sibling index for specified riak object (starts from 1)
%% RiakObject - riak object instance
%% @end
%%--------------------------------------------------------------------
getObjectSibling(SiblingIndex, RiakObject) ->
  riakc_obj:select_sibling(SiblingIndex, RiakObject).

%%--------------------------------------------------------------------
%% @doc
%% Get metadata object from specified riak object
%%
%% @spec getObjectMetadata(RiakObject) -> metadata()
%% RiakObject - riak object instance
%% @end
%%--------------------------------------------------------------------
getObjectMetadata(RiakObject) ->
  riakc_obj:get_update_metadata(RiakObject).

%%--------------------------------------------------------------------
%% @doc
%% Update metadata at specified riak object
%%
%% @spec updateObjectMetadata(RiakObject, MetadataObject) -> Riak object
%% RiakObject - riak object instance
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
updateObjectMetadata(RiakObject, MetadataObject) ->
  riakc_obj:update_metadata(RiakObject, MetadataObject).

%%--------------------------------------------------------------------
%% @doc
%% Gets metadata entry from specified riak metadata object
%%
%% @spec getMetadataEntry(MetadataObject, EntryKey) -> {metadata_key(), metadata_val()} | notfound
%% MetadataObject - metadata() object
%% EntryKey - metadata entry key
%% @end
%%--------------------------------------------------------------------
getMetadataEntry(MetadataObject, EntryKey) ->
  riakc_obj:get_user_metadata_entry(MetadataObject, EntryKey).

%%--------------------------------------------------------------------
%% @doc
%% Gets metadata entries from specified riak metadata object
%%
%% @spec getMetadataEntries(MetadataObject) -> [{metadata_key(), metadata_val()}, ...]
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
getMetadataEntries(MetadataObject) ->
  riakc_obj:get_user_metadata_entries(MetadataObject).

%%--------------------------------------------------------------------
%% @doc
%% Adds/Updates metadata entry at specified riak metadata object
%%
%% @spec setMetadataEntry(MetadataObject, EntryKey, EntryVal) -> metadata()
%% MetadataObject - metadata() object
%% EntryKey - key to add/update
%% EntryVal - Entry value
%% @end
%%--------------------------------------------------------------------
setMetadataEntry(MetadataObject, EntryKey, EntryVal) ->
  riakc_obj:set_user_metadata_entry(MetadataObject, {EntryKey, EntryVal}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes metadata entry at specified riak metadata object
%%
%% @spec deleteMetadataEntry(MetadataObject, EntryKey) -> metadata()
%% MetadataObject - metadata() object
%% EntryKey - key to delete
%% @end
%%--------------------------------------------------------------------
deleteMetadataEntry(MetadataObject, EntryKey) ->
  riakc_obj:delete_user_metadata_entry(MetadataObject, EntryKey).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all entries at specified riak metadata object
%%
%% @spec clearMetadata(MetadataObject) -> metadata()
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
clearMetadata(MetadataObject) ->
  riakc_obj:clear_user_metadata_entries(MetadataObject).

%%--------------------------------------------------------------------
%% @doc
%% Adds secondary index to specified riak metadata object
%%
%% @spec addSecondaryIndex(Metadata, Index) -> metadata()
%% MetadataObject - metadata() object
%% Index - secondary_index()
%% @end
%%--------------------------------------------------------------------
addSecondaryIndex(Metadata, Index) ->
  riakc_obj:add_secondary_index(Metadata, Index).

%%--------------------------------------------------------------------
%% @doc
%% Sets secondary index to specified riak metadata object
%%
%% @spec setSecondaryIndex(Metadata, Index) -> metadata()
%% MetadataObject - metadata() object
%% Index - secondary_index()
%% @end
%%--------------------------------------------------------------------
setSecondaryIndex(Metadata, Index) ->
  riakc_obj:set_secondary_index(Metadata, Index).

%%--------------------------------------------------------------------
%% @doc
%% Gets secondary index by ID from specified riak metadata object
%%
%% @spec getSecondaryIndex(Metadata, IndexID) -> secondary_index_value() | notfound
%% MetadataObject - metadata() object
%% IndexID - secondary_index_id()
%% @end
%%--------------------------------------------------------------------
getSecondaryIndex(Metadata, IndexID) ->
  riakc_obj:get_secondary_index(Metadata, IndexID).

%%--------------------------------------------------------------------
%% @doc
%% Gets secondary indexes from specified riak metadata object
%%
%% @spec getSecondaryIndexes(Metadata) -> [secondary_index(), ...]
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
getSecondaryIndexes(Metadata) ->
  riakc_obj:get_secondary_indexes(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Deletes secondary index by ID from specified riak metadata object
%%
%% @spec deleteSecondaryIndex(Metadata, IndexID) -> metadata()
%% MetadataObject - metadata() object
%% IndexID - secondary_index_id()
%% @end
%%--------------------------------------------------------------------
deleteSecondaryIndex(Metadata, IndexID) ->
  riakc_obj:delete_secondary_index(Metadata, IndexID).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all secondary indexes from specified riak metadata object
%%
%% @spec clearSecondaryIndexes(Metadata) -> metadata()
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
clearSecondaryIndexes(Metadata) ->
  riakc_obj:clear_secondary_indexes(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Add links for a specific tag
%%
%% @spec addLink(Metadata, SecondaryIndex) -> metadata()
%% MetadataObject - metadata() object
%% SecondaryIndex - secondary_index() | [secondary_index()]
%% @end
%%--------------------------------------------------------------------
addLink(Metadata, SecondaryIndex) ->
  riakc_obj:add_link(Metadata, SecondaryIndex).

%%--------------------------------------------------------------------
%% @doc
%% Add links for a specific tag
%%
%% @spec setLink(Metadata, Link) -> metadata()
%% MetadataObject - metadata() object
%% Link - link() | [link()]
%% @end
%%--------------------------------------------------------------------
setLink(Metadata, Link) ->
  riakc_obj:set_link(Metadata, Link).

%%--------------------------------------------------------------------
%% @doc
%% Get links for a specific tag
%%
%% @spec getLinks(Metadata, Tag)-> [id()] | notfound
%% MetadataObject - metadata() object
%% Tag - tag()
%% @end
%%--------------------------------------------------------------------
getLinks(Metadata, Tag)->
  riakc_obj:get_links(Metadata, Tag).

%%--------------------------------------------------------------------
%% @doc
%% Get all links from specified metadata
%%
%% @spec getAllLinks(Metadata) -> [link()]
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
getAllLinks(Metadata) ->
  riakc_obj:get_all_links(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Deletes links for specified tag
%%
%% @spec deleteLinks(Metadata, Tag) -> metadata()
%% MetadataObject - metadata() object
%% Tag - tag()
%% @end
%%--------------------------------------------------------------------
deleteLinks(Metadata, Tag) ->
  riakc_obj:delete_links(Metadata, Tag).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all links from specified metadata
%%
%% @spec clearLinks(Metadata) -> metadata()
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
clearLinks(Metadata) ->
  riakc_obj:clear_links(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Creates new empty set object (since Riak 2.0)
%%
%% @spec createEmptySet() -> set()
%% @end
%%--------------------------------------------------------------------
createEmptySet() ->
  riakc_set:new().

%%--------------------------------------------------------------------
%% @doc
%% Appends specified entry (entries) to specified set
%%
%% @spec addToSet(Set, Entry) -> set()
%% Set - set(), set to add data to
%% Entry - binary() | [binary()], entry to add to set
%% @end
%%--------------------------------------------------------------------
addToSet(Set, Entry) when is_list(Entry) ->
  addEntriesToSet(Entry, Set);
addToSet(Set, Entry) when is_binary(Entry) ->
  riakc_set:add_element(Entry, Set);
addToSet(_Set, _Entry) ->
  {error, badarg}.

%%--------------------------------------------------------------------
%% @doc
%% Removes specified entry (entries) from specified set
%%
%% @spec removeFromSet(Set, Entry) -> set()
%% Set - set(), set to add data to
%% Entry - binary() | [binary()], entry to remove from set
%% @end
%%--------------------------------------------------------------------
removeFromSet(Set, Entry) when is_list(Entry) ->
  removeEntriesFromSet(Entry, Set);
removeFromSet(Set, Entry) when is_binary(Entry) ->
  case riakc_set:is_element(Entry, Set) of
    true ->
      riakc_set:del_element(Entry, Set);
    _ ->
      Set
  end;
removeFromSet(_Set, _Entry) ->
  {error, badarg}.

%%--------------------------------------------------------------------
%% @doc
%% Gets specified set size
%%
%% @spec getSetSize(Set) -> integer()
%% Set - set(), set to get size
%% @end
%%--------------------------------------------------------------------
getSetSize(Set) ->
  riakc_set:size(Set).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from specified local set object
%%
%% @spec getLocalSetVal(Set) -> any()
%% Set to get value from
%% @end
%%--------------------------------------------------------------------
getLocalSetVal(Set) ->
  riakc_set:value(Set).

%%--------------------------------------------------------------------
%% @doc
%% Check specified element is set member
%%
%% @spec setContainsValue(Set, Value) -> true | false
%% @end
%%--------------------------------------------------------------------
setContainsValue(Set, Value) ->
  riakc_set:is_element(Value, Set).

%%--------------------------------------------------------------------
%% @doc
%% Creates new empty map object (since Riak 2.0)
%%
%% @spec createEmptyMap() -> map()
%% @end
%%--------------------------------------------------------------------
createEmptyMap() ->
  riakc_map:new().

%%--------------------------------------------------------------------
%% @doc
%% Stores specified entry at map. If entry with specified key exists -
%% it will be replaced
%%
%% @spec storeAtMap(Map, Entry) -> map()
%% Map - map(), map to add data to
%% Entry - term() entry to add to set
%% @end
%%--------------------------------------------------------------------
storeAtMap(Map, {register, Key, Val}) ->
  riakc_map:update({Key, register}, fun(R) -> riakc_register:set(Val, R) end, Map);
storeAtMap(Map, {flag, Operation, Key}) ->
  riakc_map:update({Key, flag}, fun(F) -> riakc_flag:Operation(F) end, Map);
storeAtMap(_Map, {counter, _Operation, _Key, Val}) when is_integer(Val) == false ->
  {error, badarg};
storeAtMap(Map, {counter, Operation, Key, Val}) ->
  riakc_map:update({Key, counter}, fun(C) -> riakc_counter:Operation(Val, C) end, Map);
storeAtMap(Map, {set, Key, Val}) ->
  riakc_map:update({Key, set}, fun(S) -> riakc_set:add_element(Val, S) end, Map);
storeAtMap(_Map, _Operation) ->
  {error, badoper}.

%%--------------------------------------------------------------------
%% @doc
%% Removes specified entry from map
%%
%% @spec removeFromMap(Map, Key) -> map()
%% Map - map(), map to remove data from
%% Key - binary(), entry key to remove
%% @end
%%--------------------------------------------------------------------
removeFromMap(Map, Key) ->
  riakc_map:erase(Key, Map).

%%--------------------------------------------------------------------
%% @doc
%% Gets entry with specified key from map. If entry with specified doesn't exists -
%% error returned
%%
%% @spec fetchFromMap(Map, Key) -> {ok, Entry} | error
%% Map - map(), map to find data at
%% Key - binary(), key assosiated with entry
%% @end
%%--------------------------------------------------------------------
fetchFromMap(Map, Key) ->
  riakc_map:find(Key, Map).

%%--------------------------------------------------------------------
%% @doc
%% Gets all keys from map
%%
%% @spec fetchMapKeys(Map) -> [binary()]
%% Map - map(), map to fetch keys from
%% @end
%%--------------------------------------------------------------------
fetchMapKeys(Map) ->
  riakc_map:fetch_keys(Map).

%%--------------------------------------------------------------------
%% @doc
%% Check map constains specified key
%%
%% @spec mapContainsKey(Map, Key) -> true | false
%% Map - map(), map to check
%% Key - binary(), key to check
%% @end
%%--------------------------------------------------------------------
mapContainsKey(Map, Key) ->
  riakc_map:is_key(Key, Map).

%%--------------------------------------------------------------------
%% @doc
%% Gets specified map size
%%
%% @spec getMapSize(Map) -> integer()
%% Map - map(), map to get size
%% @end
%%--------------------------------------------------------------------
getMapSize(Map) ->
  riakc_map:size(Map).

%%--------------------------------------------------------------------
%% @doc
%% Gets specified typed object value from specified bucket
%%
%% @spec getTypedObjVal(Type, Bucket, Key, NewValue) -> {ok, Val} | {error, Error}
%% Type - riakc_set | riakc_map - atom()
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% @end
%%--------------------------------------------------------------------
getTypedObjVal(PoolName, Type, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get_typed_obj, Type, Bucket, Key}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Gets specified typed object from specified bucket
%%
%% @spec getRawTypedObj(Bucket, Key, NewValue) -> {ok, Val} | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% @end
%%--------------------------------------------------------------------
getRawTypedObj(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get_raw_typed_obj, Bucket, Key}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Stores specified CRDT object at riak at specified bucket and key
%%
%% @spec storeTypedObject(Bucket, Key, Type, Obj) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% Type - riakc_set | riakc_map
%% @end
%%--------------------------------------------------------------------
storeTypedObject(PoolName, Bucket, Key, Type, Obj) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {store_typed_obj, Bucket, Key, Type, Obj}, infinity)
  end, infinity).

%%%===================================================================
%%% Internal functions
%%%===================================================================

encodeData(PoolName, Data) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {encode_data, Data}, infinity)
  end, infinity).

addEntriesToSet([], Set) ->
  Set;
addEntriesToSet([Entry | OtherEntries], Set) ->
  NewSet = riakc_set:add_element(Entry, Set),
  addEntriesToSet(OtherEntries, NewSet).

removeEntriesFromSet([], Set) ->
  Set;
removeEntriesFromSet([Entry | Entries], Set) ->
  case riakc_set:is_element(Entry, Set) of
    true ->
      NewSet = riakc_set:del_element(Entry, Set),
      removeEntriesFromSet(Entries, NewSet);
    _ ->
      removeEntriesFromSet(Entries, Set)
  end.
