-module(ar_coordination).

-behavior(gen_server).

-export([start_link/0, get_state/0, is_peer/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
    peers_by_partition = #{},
	peer_list = []
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
    State =
		lists:foldl(
			fun	(MiningPeer, Acc) ->
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
handle_call({is_peer, Peer}, _From, State) ->
	IsPeer = lists:member(Peer, State#state.peer_list),
	{reply, IsPeer, State};
handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

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
			fun	({PartitionSize, PartitionId, Packing}, Acc) ->
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
