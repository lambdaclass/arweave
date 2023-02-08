-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/4, set_partition/5, reset_mining_session/1, get_state/0, is_peer/1, call_remote_peer/0,
	compute_h2/2, computed_h2/1
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
	hashes_map = #{},
	timer,
	solutions_map = #{}
}).

-define(BATCH_SIZE_LIMIT, 400).
-define(BATCH_TIMEOUT_MS, 20).

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

call_remote_peer() ->
	gen_server:cast(?MODULE, call_remote_peer).

%% @doc Check if there is a peer with PartitionNumber2 and prepare the
%% coordination to send requests
set_partition(PartitionNumber2, ReplicaID, Range2End, RecallRange2Start, MiningSession) ->
	gen_server:call(
		?MODULE,
		{set_partition, PartitionNumber2, ReplicaID, Range2End, RecallRange2Start, MiningSession}
	).

%% @doc Mining session has changed. Reset it and discard any intermediate value
reset_mining_session(Ref) ->
	gen_server:call(?MODULE, {reset_mining_session, Ref}).

%% @doc Compute h2 from a remote peer
compute_h2(Peer, H2Materials) ->
	gen_server:cast(?MODULE, {compute_h2, Peer, H2Materials}).

computed_h2(Args) ->
	gen_server:cast(?MODULE, {computed_h2, Args}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	{ok, TRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
	State =
		lists:foldl(
			fun(MiningPeer, Acc) ->
				add_mining_peer(MiningPeer, Acc)
			end,
			#state{timer = TRef},
			Config#config.mining_peers
		),
	{ok, State}.

%% Helper callback to see state while testing
%% TODO Remove it
handle_call(get_state, _From, State) ->
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
handle_call({reset_mining_session, MiningSession}, _From, State) ->
	ar:console("New Mining Session ~w.~n", [MiningSession]),
	{reply, ok, State#state{
		mining_session = MiningSession, current_partition = undefined, current_peer = undefined
	}};
handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({computed_h1, CorrelationRef, Nonce, H1, MiningSession}, State) ->
	#state{hashes_map = HashesMap, mining_session = StateSession} = State,
	case StateSession == MiningSession of
		false ->
			{noreply, State};
		true ->
			NewHashesMap = maps:put({CorrelationRef, Nonce}, H1, HashesMap),
			case maps:size(NewHashesMap) >= ?BATCH_SIZE_LIMIT of
				true ->
					call_remote_peer();
				false ->
					ok
			end,
			{noreply, State#state{hashes_map = NewHashesMap}}
	end;
handle_cast(call_remote_peer, #state{hashes_map = HashesMap} = State) when map_size(HashesMap) == 0 ->
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
	{noreply, State#state{hashes_map = #{}, timer = NewTRef}};
handle_cast(call_remote_peer, #state{hashes_map = HashesMap, current_peer = Peer, current_partition = Partition, timer = TRef} = State) ->
	timer:cancel(TRef),
	call_remote_peer(Peer, Partition, maps:to_list(HashesMap)),
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
	{noreply, State#state{hashes_map = #{}, timer = NewTRef}};
handle_cast({reset_mining_session, MiningSession}, State) ->
	{noreply, State#state{
		mining_session = MiningSession, current_partition = undefined, current_peer = undefined
	}};
handle_cast({compute_h2, Peer, H2Materials}, State) ->
	ar_mining_server:remote_compute_h2(Peer, H2Materials),
	{noreply, State};
handle_cast({computed_h2, #{remote_ref := {remote, Peer, _}, materials := H2Args}}, State) ->
	SolutionsMap = maps:update_with(Peer,
									fun (V) -> [H2Args | V] end,
									[H2Args],
									State#state.solutions_map),
	State2 = State#state{ solutions_map = SolutionsMap},
	{noreply, State2};
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

call_remote_peer(Peer, Partition, HashesList) ->
	% TODO
	ar:console("Sending batch request to ~w for partition ~w of size ~w.~n", [Peer, Partition, length(HashesList)]),
	ok.
