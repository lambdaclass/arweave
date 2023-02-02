-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/4, set_partition/5, reset_mining_session/1, get_state/0, is_peer/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	peers_by_partition = #{},
	peer_list = [],
	mining_session,
	current_partition,
	current_peer,
	hashes_map = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return true if is part of the configured mining peers
is_peer(Peer) ->
	gen_server:call(?MODULE, {is_peer, Peer}).

%% Helper function to see state while testing
%% TODO Remove it
get_state() ->
	gen_server:call(?MODULE, get_state).

%% @doc An H1 has been generated. Store it to send it later to a
%% coordinated mining peer
computed_h1(CorrelationRef, Nonce, H1, MiningSession) ->
	gen_server:cast(?MODULE, {computed_h1, CorrelationRef, Nonce, H1, MiningSession}).

%% @doc Check if there is a peer with PartitionNumber2 and prepare the
%% coordination to send requests
set_partition(PartitionNumber2, ReplicaID, Range2End, RecallRange2Start, MiningSession) ->
	gen_server:cast(
		?MODULE,
		{set_partition, PartitionNumber2, ReplicaID, Range2End, RecallRange2Start, MiningSession}
	).

%% @doc Mining session has changed. Reset it and discard any intermediate value
reset_mining_session(Ref) ->
	gen_server:call(?MODULE, {reset_mining_session, Ref}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	State =
		lists:foldl(
			fun(MiningPeer, Acc) ->
				add_mining_peer(MiningPeer, Acc)
			end,
			#state{},
			Config#config.mining_peers
		),
	{ok, State}.

%% Helper callback to see state while testing
%% TODO Remove it
handle_call(get_peers, _From, State) ->
	{reply, {ok, State}, State};
handle_call(
	{set_partition, PartitionNumber2, _ReplicaID, _Range2End, _RecallRange2Start, MiningSession},
	_From,
	State
) ->
	case State#state.mining_session == MiningSession of
		false ->
			{reply, false, State};
		true ->
			case maps:find(PartitionNumber2, State#state.peers_by_partition) of
				{ok, [Peer | _]} ->
					{reply, true, State#state{
						current_partition = PartitionNumber2, current_peer = Peer
					}};
				_ ->
					{reply, false, State}
			end
	end;
handle_call({is_peer, Peer}, _From, State) ->
	IsPeer = lists:member(Peer, State#state.peer_list),
	{reply, IsPeer, State};
handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({computed_h1, CorrelationRef, Nonce, H1, MiningSession}, State) ->
	case State#state.mining_session == MiningSession of
		false ->
			{noreply, State};
		true ->
			HashesMap = State#state.hashes_map,
			NewState = State#state{hashes_map = maps:put({CorrelationRef, Nonce}, H1, HashesMap)},
			{noreply, NewState}
	end;
handle_cast({start_remote_request}, #state{hashes_map = HashesMap, current_peer = Peer, current_partition = Partition} = State) ->
	call_remote_peer(Peer, Partition, maps:to_list(HashesMap)),
	{noreply, State#state{hashes_map = #{}}};
handle_cast({reset_mining_session, MiningSession}, State) ->
	{noreply, State#state{
		mining_session = MiningSession, current_partition = undefined, current_peer = undefined
	}};
handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

add_mining_peer({Peer, StorageModules}, State) ->
	Peers = State#state.peer_list,
	MiningPeers =
		lists:foldl(
			fun({PartitionSize, PartitionId, Packing}, Acc) ->
				%% Allowing the case of same partition handled by different peers
				case maps:get(PartitionId, Acc, none) of
					L when is_list(L) ->
						maps:put(PartitionId, [{Peer, PartitionSize, Packing} | L], Acc);
					none ->
						maps:put(PartitionId, [{Peer, PartitionSize, Packing}], Acc)
				end
			end,
			State#state.peers_by_partition,
			StorageModules
		),
	State#state{peers_by_partition = MiningPeers, peer_list = [Peer | Peers]}.

call_remote_peer(_Peer, _Partition, _HashesList) ->
	% TODO
	ok.