package net.data.technology.jraft;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RaftServer implements RaftMessageHandler {

	private RaftContext context;
	private ScheduledThreadPoolExecutor scheduler;
	private ScheduledFuture<?> scheduledElection;
	private Map<Integer, PeerServer> peers = new HashMap<Integer, PeerServer>();
	private ServerRole role;
	private ServerState state;
	private int leader;
	private int id;
	private int votesResponded;
	private int votesGranted;
	private boolean electionCompleted;
	private SequentialLogStore logStore;
	private StateMachine stateMachine;
	private Logger logger;
	private Random random;
	private Callable<Void> electionTimeoutTask;
	private ClusterConfiguration config;
	
	// fields for extended messages
	private PeerServer serverToJoin = null;
	private boolean catchingUp = false;
	//TODO add more
	
	// end fields for extended messages
	
	public RaftServer(RaftContext context){
		this.id = context.getServerStateManager().getServerId();
		this.state = context.getServerStateManager().readState();
		this.logStore = context.getServerStateManager().loadLogStore();
		this.config = context.getServerStateManager().loadClusterConfiguration();
		this.stateMachine = context.getStateMachine();
		this.votesGranted = 0;
		this.votesResponded = 0;
		this.leader = -1;
		this.electionCompleted = false;
		this.context = context;
		this.logger = context.getLoggerFactory().getLogger(this.getClass());
		this.random = new Random(Calendar.getInstance().getTimeInMillis());
		this.scheduler = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
		this.electionTimeoutTask = new Callable<Void>(){

			@Override
			public Void call() throws Exception {
				handleElectionTimeout();
				return null;
			}};
		
		for(ClusterServer server : this.config.getServers()){
			if(server.getId() != this.id){
				this.peers.put(server.getId(), new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer)));
			}
		}
		
		if(this.state == null){
			this.state = new ServerState();
			this.state.setTerm(0);
			this.state.setVotedFor(-1);
		}

		this.role = ServerRole.Follower;
		this.restartElectionTimer();
		this.logger.info("Server %d started", this.id);
	}
	
	@Override
	public synchronized RaftResponseMessage processRequest(RaftRequestMessage request) {
		this.logger.debug(
				"Receive a %s message from %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d", 
				request.getMessageType().toString(),
				request.getSource(),
				request.getLastLogIndex(),
				request.getLastLogTerm(),
				request.getLogEntries() == null ? 0 : request.getLogEntries().length,
				request.getCommitIndex(),
				request.getTerm());
		if(request.getMessageType() == RaftMessageType.AppendEntriesRequest || request.getMessageType() == RaftMessageType.RequestVoteRequest){
			// we allow the server to be continue after term updated to save a round message
			this.updateTerm(request.getTerm());
		}
	
		RaftResponseMessage response = null;
		if(request.getMessageType() == RaftMessageType.AppendEntriesRequest){
			response = this.handleAppendEntriesRequest(request);
		}else if(request.getMessageType() == RaftMessageType.RequestVoteRequest){
			response = this.handleVoteRequest(request);
		}else if(request.getMessageType() == RaftMessageType.ClientRequest){
			response = this.handleClientRequest(request);
		}else{
			// extended requests
			response = this.handleExtendedMessages(request);
		}
		
		if(response != null){
			this.logger.debug(
					"Response back a %s message to %d with Accepted=%s, Term=%d, NextIndex=%d",
					response.getMessageType().toString(),
					response.getDestination(),
					String.valueOf(response.isAccepted()),
					response.getTerm(),
					response.getNextIndex());
		}
		
		return response;
	}
	
	private RaftResponseMessage handleAppendEntriesRequest(RaftRequestMessage request){
		if(this.catchingUp){
			this.logger.debug("stop log syncing mode");
			this.catchingUp = false;
		}
		
		if(request.getTerm() == this.state.getTerm()){
			if(this.role == ServerRole.Candidate){
				this.becomeFollower();
			}else if(this.role == ServerRole.Leader){
				this.logger.error("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits", request.getSource());
				System.exit(-1);
			}else{
				this.restartElectionTimer();
			}
		}
		
		RaftResponseMessage response = new RaftResponseMessage();
		response.setMessageType(RaftMessageType.AppendEntriesResponse);
		response.setTerm(this.state.getTerm());
		response.setSource(this.id);
		response.setDestination(request.getSource());
		boolean logOkay = request.getLastLogIndex() == 0 ||
				(request.getLastLogIndex() < this.logStore.getFirstAvailableIndex() && 
						request.getLastLogTerm() == this.logStore.getLogEntryAt(request.getLastLogIndex()).getTerm());
		if(request.getTerm() < this.state.getTerm() || !logOkay){
			response.setAccepted(false);
			response.setNextIndex(0);
			return response;
		}
		
		// The role is Follower and log is okay now
		if(request.getLogEntries() != null && request.getLogEntries().length > 0){
			// write the logs to the store, first of all, check for overlap, and skip them
			LogEntry[] logEntries = request.getLogEntries();
			long index = request.getLastLogIndex() + 1;
			int logIndex = 0;
			while(index < this.logStore.getFirstAvailableIndex() && 
					logIndex < logEntries.length && 
					logEntries[logIndex].getTerm() == this.logStore.getLogEntryAt(index).getTerm()){
				logIndex ++;
				index ++;
			}
			
			// dealing with overwrites
			while(index < this.logStore.getFirstAvailableIndex() && logIndex < logEntries.length){
				if(index <= this.config.getLogIndex()){
					// we need to restore the configuration from previous committed configuration
					ClusterConfiguration newConfig = this.context.getServerStateManager().loadClusterConfiguration();
					this.reconfigure(newConfig);
				}
				this.logStore.writeAt(index ++, logEntries[logIndex ++]);
			}
			
			// append the new log entries
			while(logIndex < logEntries.length){
				LogEntry logEntry = logEntries[logIndex ++];
				this.logStore.append(logEntry);
				if(logEntry.getValueType() == LogValueType.Configuration){
					ClusterConfiguration newConfig = ClusterConfiguration.fromBytes(logEntry.getValue());
					this.reconfigure(newConfig);
				}
			}
		}
		
		this.leader = request.getSource();
		this.commit(request.getCommitIndex());
		response.setAccepted(true);
		response.setNextIndex(request.getLastLogIndex() + (request.getLogEntries() == null ? 0 : request.getLogEntries().length) + 1);
		return response;
	}
	
	private RaftResponseMessage handleVoteRequest(RaftRequestMessage request){
		RaftResponseMessage response = new RaftResponseMessage();
		response.setMessageType(RaftMessageType.RequestVoteResponse);
		response.setSource(this.id);
		response.setDestination(request.getSource());
		response.setTerm(this.state.getTerm());
		
		boolean logOkay = request.getLastLogTerm() > this.logStore.getLastLogEntry().getTerm() ||
				(request.getLastLogTerm() == this.logStore.getLastLogEntry().getTerm() &&
			     this.logStore.getFirstAvailableIndex() - 1 <= request.getLastLogIndex());
		boolean grant = request.getTerm() == this.state.getTerm() && logOkay && (this.state.getVotedFor() == request.getSource() || this.state.getVotedFor() == -1);
		response.setAccepted(grant);
		if(grant){
			this.state.setVotedFor(request.getSource());
			this.context.getServerStateManager().persistState(this.state);
		}
		
		return response;
	}
	
	private RaftResponseMessage handleClientRequest(RaftRequestMessage request){
		RaftResponseMessage response = new RaftResponseMessage();
		response.setMessageType(RaftMessageType.AppendEntriesResponse);
		response.setSource(this.id);
		response.setDestination(this.leader);
		response.setTerm(this.state.getTerm());
		
		if(this.role != ServerRole.Leader){
			response.setAccepted(false);
			return response;
		}
		
		LogEntry[] logEntries = request.getLogEntries();
		if(logEntries != null && logEntries.length > 0){
			for(int i = 0; i < logEntries.length; ++i){
				this.logStore.append(new LogEntry(this.state.getTerm(), logEntries[i].getValue()));
			}
		}
		
		// Urgent commit, so that the commit will not depend on heartbeat
		this.requestAppendEntries(true);
		response.setAccepted(true);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		return response;
	}

	private synchronized void handleElectionTimeout(){
		if(this.role == ServerRole.Leader){
			this.logger.error("A leader should never encounter election timeout, illegal application state, stop the application");
			System.exit(-1);
		}
		
		this.logger.debug("Election timeout, change to Candidate");
		this.state.increaseTerm();
		this.state.setVotedFor(-1);
		this.role = ServerRole.Candidate;
		this.votesGranted = 0;
		this.votesResponded = 0;
		this.electionCompleted = false;
		this.context.getServerStateManager().persistState(this.state);
		this.requestVote();
		
		// restart the election timer if this is not yet a leader
		if(this.role != ServerRole.Leader){
			this.restartElectionTimer();
		}
	}
	
	private void requestVote(){
		// vote for self
		this.logger.info("requestVote started with term %d", this.state.getTerm());
		this.state.setVotedFor(this.id);
		this.context.getServerStateManager().persistState(this.state);
		this.votesGranted += 1;
		this.votesResponded += 1;
		
		// this is the only server?
		if(this.votesGranted > (this.peers.size() + 1) / 2){
			this.electionCompleted = true;
			this.becomeLeader();
			return;
		}
		
		LogEntry lastLogEntry = this.logStore.getLastLogEntry();
		for(PeerServer peer : this.peers.values()){
			RaftRequestMessage request = new RaftRequestMessage();
			request.setMessageType(RaftMessageType.RequestVoteRequest);
			request.setDestination(peer.getId());
			request.setSource(this.id);
			request.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
			request.setLastLogTerm(lastLogEntry == null ? 0 : lastLogEntry.getTerm());
			request.setTerm(this.state.getTerm());
			this.logger.debug("send %s to server %d with term %d", RaftMessageType.RequestVoteRequest.toString(), peer.getId(), this.state.getTerm());
			peer.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
				handlePeerResponse(response, error);
			});
		}
	}
	
	private void requestAppendEntries(boolean urgentRequest){
		for(PeerServer peer : this.peers.values()){
			if(urgentRequest || !peer.isBusy()){
				peer.SendRequest(this.createAppendEntriesRequest(peer))
					.whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
						handlePeerResponse(response, error);
					});
			}
		}
	}
	
	private synchronized void handlePeerResponse(RaftResponseMessage response, Throwable error){
		if(error != null){
			this.logger.info("peer response error: %s", error.getMessage());
			return;
		}
		
		this.logger.debug(
				"Receive a %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
				response.getMessageType().toString(),
				response.getSource(),
				String.valueOf(response.isAccepted()),
				response.getTerm(),
				response.getNextIndex());
		// If term is updated no need to proceed
		if(this.updateTerm(response.getTerm())){
			return;
		}
		
		// Ignore the response that with lower term for safety
		if(response.getTerm() < this.state.getTerm()){
			this.logger.info("Received a peer response from %d that with lower term value %d v.s. %d", response.getSource(), response.getTerm(), this.state.getTerm());
			return;
		}
		
		if(response.getMessageType() == RaftMessageType.RequestVoteResponse){
			this.handleVotingResponse(response);
		}else if(response.getMessageType() == RaftMessageType.AppendEntriesResponse){
			this.handleAppendEntriesResponse(response);
		}else{
			this.logger.error("Received an unexpected message %s for response, system exits.", response.getMessageType().toString());
			System.exit(-1);
		}
	}
	
	private void handleAppendEntriesResponse(RaftResponseMessage response){
		PeerServer peer = this.peers.get(response.getSource());
		if(peer == null){
			this.logger.info("the response is from an unkonw peer %d", response.getSource());
			return;
		}
		
		if(response.isAccepted()){
			synchronized(peer){
				if(this.role == ServerRole.Leader && !peer.isHeartbeatEnabled()){
					// Enable heartbeat for this peer and start the heartbeat
					this.enableHeartbeatForPeer(peer);
				}
				
				peer.setNextLogIndex(response.getNextIndex());
				peer.setMatchedIndex(response.getNextIndex() - 1);
			}
			
			// try to commit with this response
			this.tryToCommit();
		}else{
			synchronized(peer){
				// Stop heartbeating until this peer catches up all the logs
				if(peer.isHeartbeatEnabled()){
					peer.enableHeartbeat(false);
				}
				
				peer.setNextLogIndex(peer.getNextLogIndex() - 1);
			}
			
			// This may not be a leader anymore, such as the response was sent out long time ago
	        // and the role was updated by UpdateTerm call
	        // Try to match up the logs for this peer
			if(this.role == ServerRole.Leader){
				this.requestAppendEntries(false);
			}
		}
	}
	
	private void handleVotingResponse(RaftResponseMessage response){
		this.votesResponded += 1;
		if(this.electionCompleted){
			this.logger.info("Election completed, will ignore the voting result from this server");
			return;
		}
		
		if(response.isAccepted()){
			this.votesGranted += 1;
		}
		
		if(this.votesResponded >= this.peers.size() + 1){
			this.electionCompleted = true;
		}
		
		// got a majority set of granted votes
		if(this.votesGranted > (this.peers.size() + 1) / 2){
			this.logger.info("Server is elected as leader for term %d", this.state.getTerm());
			this.electionCompleted = true;
			this.becomeLeader();
		}
	}
	
	private synchronized void handleHeartbeatTimeout(PeerServer peer){
		this.logger.debug("Heartbeat timeout for %d", peer.getId());
		if(this.role == ServerRole.Leader){
			if(!peer.isBusy()){
				peer.SendRequest(this.createAppendEntriesRequest(peer))
				.whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
					handlePeerResponse(response, error);
				});
			}
			
			synchronized(peer){
				if(peer.isHeartbeatEnabled()){
					// Schedule another heartbeat if heartbeat is still enabled 
					peer.setHeartbeatTask(this.scheduler.schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
				}else{
					this.logger.debug("heartbeat is disabled for peer %d", peer.getId());
				}
			}
		}else{
			this.logger.info("Receive a heartbeat event for %d while no longer as a leader", peer.getId());
		}
	}
	
	private void restartElectionTimer(){
		if(this.scheduledElection != null){
			this.scheduledElection.cancel(false);
		}

		this.scheduleElectionTimeout();
	}
	
	private void stopElectionTimer(){
		if(this.scheduledElection == null){
			this.logger.warning("Election Timer is never started but is requested to stop, protential a bug");
			return;
		}
		
		this.scheduledElection.cancel(false);
		this.scheduledElection = null;
	}
	
	private void scheduleElectionTimeout(){
		RaftParameters parameters = this.context.getRaftParameters();
		int electionTimeout = parameters.getElectionTimeoutLowerBound() + this.random.nextInt(parameters.getElectionTimeoutUpperBound() - parameters.getElectionTimeoutLowerBound() + 1);
		this.scheduledElection = this.scheduler.schedule(this.electionTimeoutTask, electionTimeout, TimeUnit.MILLISECONDS);
	}
	
	private void becomeLeader(){
		this.stopElectionTimer();
		this.role = ServerRole.Leader;
		this.leader = this.id;
		for(PeerServer server : this.peers.values()){
			this.enableHeartbeatForPeer(server);
		}
		
		// if current config is not committed, try to commit it
		if(this.config.getLogIndex() == 0){
			this.config.setLogIndex(this.logStore.getFirstAvailableIndex());
			this.logStore.append(new LogEntry(this.state.getTerm(), this.config.toBytes(), LogValueType.Configuration));
		}
		
		this.requestAppendEntries(true);
	}
	
	private void enableHeartbeatForPeer(PeerServer peer){
		peer.enableHeartbeat(true);
		peer.resumeHeartbeatingSpeed();
		peer.setHeartbeatTask(this.scheduler.schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
	}
	
	private void becomeFollower(){
		// stop heartbeat for all peers
		for(PeerServer server : this.peers.values()){
			server.enableHeartbeat(false);
			if(server.getHeartbeatTask() != null){
				server.getHeartbeatTask().cancel(false);
			}
		}
		
		this.role = ServerRole.Follower;
		this.restartElectionTimer();
	}
	
	private boolean updateTerm(long term){
		if(term > this.state.getTerm()){
			this.state.setTerm(term);
			this.state.setVotedFor(-1);
			this.electionCompleted = false;
			this.votesGranted = 0;
			this.votesResponded = 0;
			this.context.getServerStateManager().persistState(this.state);
			this.becomeFollower();
			return true;
		}
		
		return false;
	}
	
	private void commit(long targetIndex){
		if(targetIndex > this.state.getCommitIndex()){
			while(this.state.getCommitIndex() < targetIndex && this.state.getCommitIndex() < this.logStore.getFirstAvailableIndex() - 1){
				long indexToCommit = this.state.getCommitIndex() + 1;
				LogEntry logEntry = this.logStore.getLogEntryAt(indexToCommit);
				if(logEntry.getValueType() == LogValueType.Application){
					this.stateMachine.commit(indexToCommit, this.logStore.getLogEntryAt(indexToCommit).getValue());
				}else if(logEntry.getValueType() == LogValueType.Configuration){
					this.context.getServerStateManager().saveClusterConfiguration(this.config);
				}
				
				this.state.setCommitIndex(indexToCommit);
			}
			
			// save the commitment state
			this.context.getServerStateManager().persistState(this.state);
			
			// Ask peers to commit the value
			if(this.role == ServerRole.Leader){
				this.requestAppendEntries(true);
			}
		}
	}
	
	private RaftRequestMessage createAppendEntriesRequest(PeerServer peer){
		long currentNextIndex = 0;
		long commitIndex = 0;
		long lastLogIndex = 0;
		long term = 0;
		
		synchronized(this){
			currentNextIndex = this.logStore.getFirstAvailableIndex();
			commitIndex = this.state.getCommitIndex();
			term = this.state.getTerm();
		}
		
		synchronized(peer){
			if(peer.getNextLogIndex() == 0){
				peer.setNextLogIndex(currentNextIndex);
			}
			
			lastLogIndex = peer.getNextLogIndex() - 1;
		}
		
		if(lastLogIndex >= currentNextIndex){
			this.logger.error("Peer's lastLogIndex is too large %d v.s. %d, server exits", lastLogIndex, currentNextIndex);
			System.exit(-1);
		}
		
		LogEntry lastLogEntry = lastLogIndex > 0 ? this.logStore.getLogEntryAt(lastLogIndex) : null;
		LogEntry[] logEntries = (lastLogIndex + 1) >= currentNextIndex ? null : this.logStore.getLogEntries(lastLogIndex + 1, currentNextIndex);
		this.logger.debug(
				"An AppendEntries Request for %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d", 
				peer.getId(),
				lastLogIndex,
				lastLogEntry == null ? 0 : lastLogEntry.getTerm(),
				logEntries == null ? 0 : logEntries.length,
				commitIndex,
				term);
		RaftRequestMessage requestMessage = new RaftRequestMessage();
		requestMessage.setMessageType(RaftMessageType.AppendEntriesRequest);
		requestMessage.setSource(this.id);
		requestMessage.setDestination(peer.getId());
		requestMessage.setLastLogIndex(lastLogIndex);
		requestMessage.setLastLogTerm(lastLogEntry == null ? 0 : lastLogEntry.getTerm());
		requestMessage.setLogEntries(logEntries);
		requestMessage.setCommitIndex(commitIndex);
		requestMessage.setTerm(term);
		return requestMessage;
	}
	
	private void tryToCommit(){
		ArrayList<Long> matchedIndexes = new ArrayList<Long>(this.peers.size() + 1);
		matchedIndexes.add(this.logStore.getFirstAvailableIndex() - 1);
		for(PeerServer peer : this.peers.values()){
			matchedIndexes.add(peer.getMatchedIndex());
		}
		
		matchedIndexes.sort(new Comparator<Long>(){

			@Override
			public int compare(Long arg0, Long arg1) {
				return (int)(arg0.longValue() - arg1.longValue());
			}});
		this.commit(matchedIndexes.get((this.peers.size() + 1) / 2));
	}
	
	private void reconfigure(ClusterConfiguration newConfig){
		// according to our design, the new configuration never send to a server that is removed
		// so, in this method, we are not considering self get removed scenario
		this.logger.debug(
				"system is reconfigured to have %d servers, last config index: %d, this config index: %d", 
				newConfig.getServers().size(),
				newConfig.getLastLogIndex(),
				newConfig.getLogIndex());
		List<Integer> serversRemoved = new LinkedList<Integer>();
		List<ClusterServer> serversAdded = new LinkedList<ClusterServer>();
		for(ClusterServer s : newConfig.getServers()){
			if(!this.peers.containsKey(s.getId()) && s.getId() != this.id){
				serversAdded.add(s);
			}
		}
		
		for(Integer id : this.peers.keySet()){
			boolean removed = true;
			for(ClusterServer s : newConfig.getServers()){
				if(s.getId() == id.intValue()){
					removed = false;
					break;
				}
			}
			
			if(removed){
				serversRemoved.add(id);
			}
		}
		
		for(ClusterServer server : serversAdded){
			if(server.getId() != this.id){
				PeerServer peer = new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer));
				this.peers.put(server.getId(), peer);
				this.logger.info("server %d is added to cluster", peer.getId());
				if(this.role == ServerRole.Leader){
					this.logger.info("enable heartbeating for server %d", peer.getId());
					this.enableHeartbeatForPeer(peer);
				}
			}
		}
		
		for(Integer id : serversRemoved){
			PeerServer peer = this.peers.get(id);
			peer.enableHeartbeat(false);
			if(peer.getHeartbeatTask() != null){
				peer.getHeartbeatTask().cancel(false);
			}
			
			this.peers.remove(id);
			this.logger.info("server %d is removed from cluster", id.intValue());
		}
		
		this.config = newConfig;
	}
	
	private RaftResponseMessage handleExtendedMessages(RaftRequestMessage request){
		if(request.getMessageType() == RaftMessageType.AddServerRequest){
			return this.handleAddServerRequest(request);
		}else if(request.getMessageType() == RaftMessageType.RemoveServerRequest){
			return this.handleRemoveServerRequest(request);
		}else if(request.getMessageType() == RaftMessageType.SyncLogRequest){
			return this.handleLogSyncRequest(request);
		}else if(request.getMessageType() == RaftMessageType.JoinClusterRequest){
			return this.handleJoinClusterRequest(request);
		}else if(request.getMessageType() == RaftMessageType.LeaveClusterRequest){
			//TODO handle LeaveClusterRequest
		}
		
		return null;
	}
	
	private void handleExtendedResponse(RaftResponseMessage response, Throwable error){
		if(error != null){
			this.handleExtendedResponseError(error);
			return;
		}
		
		this.logger.debug(
				"Receive an extended %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
				response.getMessageType().toString(),
				response.getSource(),
				String.valueOf(response.isAccepted()),
				response.getTerm(),
				response.getNextIndex());
		if(response.getMessageType() == RaftMessageType.SyncLogResponse){
			if(this.serverToJoin != null){
				// we are reusing heartbeat interval value to indicate when to stop retry
				this.serverToJoin.resumeHeartbeatingSpeed();
				this.syncLogsToNewComingServer(response.getNextIndex()); // sync from the very first log entry
			}
		}else if(response.getMessageType() == RaftMessageType.JoinClusterResponse){
			if(this.serverToJoin != null){
				if(response.isAccepted()){
					this.logger.debug("new server confirms it will join, start syncing logs to it");
					this.syncLogsToNewComingServer(1);
				}else{
					this.logger.debug("new server cannot accept the invitation, give up");
				}
			}else{
				this.logger.debug("no server to join, drop the message");
			}
		}
		// TODO handle all extended responses
		// for SyncLogResponse
	}
	
	private void handleExtendedResponseError(Throwable error){
		this.logger.info("receive an error response from peer server, %s", error.toString());
		RpcException rpcError = null;
		if(error instanceof RpcException){
			rpcError = (RpcException)error;
		}else if(error instanceof CompletionException && ((CompletionException)error).getCause() instanceof RpcException){
			rpcError = (RpcException)((CompletionException)error).getCause();
		}
		
		if(rpcError != null){
			this.logger.debug("it's a rpc error, see if we need to retry");
			final RaftRequestMessage request = rpcError.getRequest();
			if(request.getMessageType() == RaftMessageType.SyncLogRequest || request.getMessageType() == RaftMessageType.JoinClusterRequest){
				final PeerServer server = this.serverToJoin;
				if(server != null){
					if(server.getCurrentHeartbeatInterval() >= this.context.getRaftParameters().getMaxHeartbeatInterval()){
						this.logger.info("rpc failed again for the new coming server (%d), will stop retry for this server", server.getId());
						this.serverToJoin = null;
					}else{
						// reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
						this.logger.debug("retry the request");
						server.slowDownHeartbeating();
						final RaftServer self = this;
						this.scheduler.schedule(new Callable<Void>(){

							@Override
							public Void call() throws Exception {
								self.logger.debug("retrying the request %s", request.getMessageType().toString());
								server.SendRequest(request).whenCompleteAsync((RaftResponseMessage furtherResponse, Throwable furtherError) -> {
									self.handleExtendedResponse(furtherResponse, furtherError);
								});
								return null;
							}}, server.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS);
					}
				}
			}
		}
	}
	
	private RaftResponseMessage handleRemoveServerRequest(RaftRequestMessage request){
		LogEntry[] logEntries = request.getLogEntries();
		RaftResponseMessage response = new RaftResponseMessage();
		response.setSource(this.id);
		response.setDestination(this.leader);
		response.setTerm(this.state.getTerm());
		response.setMessageType(RaftMessageType.RemoveServerResponse);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(false);
		if(logEntries.length != 1 || logEntries[0].getValue() == null || logEntries[0].getValue().length != Integer.BYTES){
			this.logger.info("bad remove server request as we are expecting one log entry with value type of Integer");
			return response;
		}
		
		if(this.role != ServerRole.Leader){
			this.logger.info("this is not a leader, cannot handle RemoveServerRequest");
			return response;
		}
		
		int serverId = ByteBuffer.wrap(logEntries[0].getValue()).getInt();
		if(serverId == this.id){
			this.logger.info("cannot request to remove leader");
			return response;
		}
		
		PeerServer peer = this.peers.get(serverId);
		if(peer == null){
			this.logger.info("server %d does not exist", serverId);
			return response;
		}
		
		ClusterConfiguration newConfig = new ClusterConfiguration();
		newConfig.setLastLogIndex(this.config.getLogIndex());
		newConfig.setLogIndex(this.logStore.getFirstAvailableIndex());
		for(ClusterServer server: this.config.getServers()){
			if(server.getId() != serverId){
				newConfig.getServers().add(server);
			}
		}
		
		this.logStore.append(new LogEntry(this.state.getTerm(), newConfig.toBytes(), LogValueType.Configuration));
		this.reconfigure(newConfig);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(true);
		return response;
	}
	
	private RaftResponseMessage handleAddServerRequest(RaftRequestMessage request){
		LogEntry[] logEntries = request.getLogEntries();
		RaftResponseMessage response = new RaftResponseMessage();
		response.setSource(this.id);
		response.setDestination(this.leader);
		response.setTerm(this.state.getTerm());
		response.setMessageType(RaftMessageType.AddServerResponse);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(false);
		if(logEntries.length != 1 || logEntries[0].getValueType() != LogValueType.ClusterServer){
			this.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer");
			return response;
		}

		if(this.role != ServerRole.Leader){
			this.logger.info("this is not a leader, cannot handle AddServerRequest");
			return response;
		}
		
		ClusterServer server = new ClusterServer(ByteBuffer.wrap(logEntries[0].getValue()));
		if(this.peers.containsKey(server.getId()) || this.id == server.getId()){
			this.logger.warning("the server to be added has a duplicated id with existing server %d", server.getId());
			return response;
		}
		
		if(this.config.getLogIndex() == 0 || this.config.getLogIndex() > this.state.getCommitIndex()){
			// the previous config has not committed yet
			this.logger.info("previous config has not committed yet");
			return response;
		}
		
		this.serverToJoin = new PeerServer(server, this.context, peerServer -> {
			this.handleHeartbeatTimeout(peerServer);
		});

		this.inviteServerToJoinCluster();
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(true);
		return response;
	}

	private RaftResponseMessage handleLogSyncRequest(RaftRequestMessage request){
		LogEntry[] logEntries = request.getLogEntries();
		RaftResponseMessage response = new RaftResponseMessage();
		response.setSource(this.id);
		response.setDestination(request.getSource());
		response.setTerm(this.state.getTerm());
		response.setMessageType(RaftMessageType.SyncLogResponse);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(false);
		if(logEntries == null || 
				logEntries.length != 1 ||
				logEntries[0].getValueType() != LogValueType.LogPack ||
				logEntries[0].getValue() == null || 
				logEntries[0].getValue().length == 0){
			this.logger.info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements");
			return response;
		}
		
		if(!this.catchingUp){
			this.logger.debug("This server is ready for cluster, ignore the request");
			return response;
		}
		
		this.logStore.applyLogPack(request.getLastLogIndex() + 1, logEntries[0].getValue());
		this.commit(this.logStore.getFirstAvailableIndex() -1);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(true);
		return response;
	}
	
	private void syncLogsToNewComingServer(long startIndex){
		// only sync committed logs
		int gap = (int)(this.state.getCommitIndex() - startIndex);
		if(gap < this.context.getRaftParameters().getLogSyncStopGap()){
			
			this.logger.info("LogSync is done for server %d with log gap %d, now put the server into cluster", this.serverToJoin.getId(), gap);
			ClusterConfiguration newConfig = new ClusterConfiguration();
			newConfig.setLastLogIndex(this.config.getLogIndex());
			newConfig.setLogIndex(this.logStore.getFirstAvailableIndex());
			newConfig.getServers().addAll(this.config.getServers());
			newConfig.getServers().add(this.serverToJoin.getClusterConfig());
			LogEntry configEntry = new LogEntry(this.state.getTerm(), newConfig.toBytes(), LogValueType.Configuration);
			this.logStore.append(configEntry);
			this.config = newConfig;
			this.peers.put(this.serverToJoin.getId(), this.serverToJoin);
			this.enableHeartbeatForPeer(this.serverToJoin);
			this.serverToJoin = null;
			return;
		}
		
		int sizeToSync = Math.min(gap, this.context.getRaftParameters().getLogSyncBatchSize());
		byte[] logPack = this.logStore.packLog(startIndex, sizeToSync);
		RaftRequestMessage request = new RaftRequestMessage();
		request.setCommitIndex(this.state.getCommitIndex());
		request.setDestination(this.serverToJoin.getId());
		request.setSource(this.id);
		request.setTerm(this.state.getTerm());
		request.setMessageType(RaftMessageType.SyncLogRequest);
		request.setLastLogIndex(startIndex - 1);
		request.setLogEntries(new LogEntry[] { new LogEntry(this.state.getTerm(), logPack, LogValueType.LogPack) });
		this.serverToJoin.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
			this.handleExtendedResponse(response, error);
		});
	}
	
	private void inviteServerToJoinCluster(){
		RaftRequestMessage request = new RaftRequestMessage();
		request.setCommitIndex(this.state.getCommitIndex());
		request.setDestination(this.serverToJoin.getId());
		request.setSource(this.id);
		request.setTerm(this.state.getTerm());
		request.setMessageType(RaftMessageType.JoinClusterRequest);
		request.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
		request.setLogEntries(new LogEntry[] { new LogEntry(this.state.getTerm(), this.config.toBytes(), LogValueType.Configuration) });
		this.serverToJoin.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
			this.handleExtendedResponse(response, error);
		});
	}

	private RaftResponseMessage handleJoinClusterRequest(RaftRequestMessage request){
		LogEntry[] logEntries = request.getLogEntries();
		RaftResponseMessage response = new RaftResponseMessage();
		response.setSource(this.id);
		response.setDestination(request.getSource());
		response.setTerm(this.state.getTerm());
		response.setMessageType(RaftMessageType.JoinClusterResponse);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(false);
		if(logEntries == null || 
				logEntries.length != 1 ||
				logEntries[0].getValueType() != LogValueType.Configuration ||
				logEntries[0].getValue() == null || 
				logEntries[0].getValue().length == 0){
			this.logger.info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements");
			return response;
		}
		
		if(this.catchingUp){
			this.logger.info("this server is already in log syncing mode");
			return response;
		}
		
		this.catchingUp = true;
		this.role = ServerRole.Follower;
		this.leader = request.getSource();
		this.state.setCommitIndex(0);
		this.state.setVotedFor(-1);
		this.context.getServerStateManager().persistState(this.state);
		this.stopElectionTimer();
		ClusterConfiguration newConfig = ClusterConfiguration.fromBytes(logEntries[0].getValue());
		this.reconfigure(newConfig);
		response.setAccepted(true);
		return response;
	}
}
