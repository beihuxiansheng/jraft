package net.data.technology.jraft;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
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
	private long commitIndex;
	private SequentialLogStore logStore;
	private StateMachine stateMachine;
	private Logger logger;
	private Random random;
	private Callable<Void> electionTimeoutTask;
	
	public RaftServer(RaftContext context, ClusterConfiguration configuration){
		this.id = configuration.getLocalServerId();
		this.state = context.getServerStateManager().readState(configuration.getLocalServerId());
		this.logStore = context.getServerStateManager().loadLogStore(configuration.getLocalServerId());
		this.stateMachine = context.getStateMachine();
		this.commitIndex = 0;
		this.votesGranted = 0;
		this.votesResponded = 0;
		this.leader = -1;
		this.electionCompleted = false;
		this.context = context;
		this.logger = context.getLoggerFactory().getLogger(this.getClass());
		this.random = new Random(Calendar.getInstance().getTimeInMillis());
		this.scheduler = new ScheduledThreadPoolExecutor(configuration.getServers().size() + 1);
		this.electionTimeoutTask = new Callable<Void>(){

			@Override
			public Void call() throws Exception {
				handleElectionTimeout();
				return null;
			}};
		
		for(ClusterServer server : configuration.getServers()){
			this.peers.put(server.getId(), new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer)));
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
	public RaftResponseMessage processRequest(RaftRequestMessage request) {
		this.logger.debug(
				"Receive a %s message from %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d", 
				request.getMessageType().toString(),
				request.getSource(),
				request.getLastLogIndex(),
				request.getLastLogTerm(),
				request.getLogEntries() == null ? 0 : request.getLogEntries().length,
				request.getCommitIndex(),
				request.getTerm());
		synchronized(this){
			if(request.getMessageType() != RaftMessageType.ClientRequest){
				// we allow the server to be continue after term updated to save a round message
				this.updateTerm(request.getTerm());
			}
		}
		
		RaftResponseMessage response = null;
		if(request.getMessageType() == RaftMessageType.AppendEntriesRequest){
			response = this.handleAppendEntriesRequest(request);
		}else if(request.getMessageType() == RaftMessageType.RequestVoteRequest){
			response = this.handleVoteRequest(request);
		}else if(request.getMessageType() == RaftMessageType.ClientRequest){
			response = this.handleClientRequest(request);
		}else{
			// should never happen, just in case
			this.logger.error("Received an unexpected message %s for request, system exits.", request.getMessageType().toString());
			System.exit(-1);
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
			
			// dealing with over writes
			while(index < this.logStore.getFirstAvailableIndex() && logIndex < logEntries.length){
				this.logStore.writeAt(index ++, logEntries[logIndex ++]);
			}
			
			// append the new log entries
			while(logIndex < logEntries.length){
				this.logStore.append(logEntries[logIndex ++]);
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
			this.context.getServerStateManager().persistState(this.id, this.state);
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
		this.context.getServerStateManager().persistState(this.id, this.state);
		this.requestVote();
		this.restartElectionTimer();
	}
	
	private void requestVote(){
		// vote for self
		this.logger.info("requestVote started with term %d", this.state.getTerm());
		this.state.setVotedFor(this.id);
		this.context.getServerStateManager().persistState(this.id, this.state);
		this.votesGranted += 1;
		this.votesResponded += 1;
		
		// this is the only server?
		if(this.votesGranted > (this.peers.size() + 1) / 2){
			this.electionCompleted = true;
			this.becomeLeader();
		}
		
		LogEntry lastLogEntry = this.logStore.getLastLogEntry();
		final RequestContext requestContext = new RequestContext(this.peers.size() + 1);
		for(PeerServer peer : this.peers.values()){
			RaftRequestMessage request = new RaftRequestMessage();
			request.setMessageType(RaftMessageType.RequestVoteRequest);
			request.setDestination(peer.getId());
			request.setSource(this.id);
			request.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
			request.setLastLogTerm(lastLogEntry == null ? 0 : lastLogEntry.getTerm());
			request.setTerm(this.state.getTerm());
			peer.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
				handlePeerResponse(response, error, requestContext);
			});
		}
	}
	
	@SuppressWarnings("unchecked")
	private void requestAppendEntries(boolean urgentRequest){
		RequestContext requestContext = new RequestContext(this.peers.size() + 1, new ArrayList<Long>());
		((List<Long>)requestContext.getState()).add(this.logStore.getFirstAvailableIndex());
		
		for(PeerServer peer : this.peers.values()){
			if(urgentRequest || !peer.isBusy()){
				peer.SendRequest(this.createAppendEntriesRequest(peer))
					.whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
						handlePeerResponse(response, error, requestContext);
					});
			}
		}
	}
	
	private void handlePeerResponse(RaftResponseMessage response, Throwable error, RequestContext context){
		if(error != null){
			this.logger.info("peer response error: %s", error.getMessage());
			
			// we might still be able to commit if there is already a majority set
			this.tryToCommit(context, 0);
			return;
		}
		
		this.logger.debug(
				"Receive a %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
				response.getMessageType().toString(),
				response.getSource(),
				String.valueOf(response.isAccepted()),
				response.getTerm(),
				response.getNextIndex());
		synchronized(this){
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
				this.handleVotingResponse(response, context);
			}else if(response.getMessageType() == RaftMessageType.AppendEntriesResponse){
				this.handleAppendEntriesResponse(response, context);
			}else{
				this.logger.error("Received an unexpected message %s for response, system exits.", response.getMessageType().toString());
				System.exit(-1);
			}
		}
	}
	
	private void handleAppendEntriesResponse(RaftResponseMessage response, RequestContext context){
		PeerServer peer = this.peers.get(response.getSource());
		if(peer == null){
			this.logger.info("the response is from an unkonw peer %d", response.getSource());
			return;
		}
		
		if(response.isAccepted()){
			synchronized(peer){
				if(this.role == ServerRole.Leader && !peer.isHeartbeatEnabled()){
					// Enable heartbeat for this peer and start the heartbeat
					peer.enableHeartbeat(true);
					peer.resumeHeartbeatingSpeed();
					peer.setHeartbeatTask(this.scheduler.schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
				}
				
				peer.setNextLogIndex(response.getNextIndex());
			}
		}else{
			synchronized(peer){
				// Stop heartbeating until this peer catches up all the logs
				if(peer.isHeartbeatEnabled()){
					peer.enableHeartbeat(false);
				}
				
				peer.setNextLogIndex(peer.getNextLogIndex() - 1);
			}
		}
		
		// This may not be a leader anymore, such as the response was sent out long time ago
        // and the role was updated by UpdateTerm call
        // Try to match up the logs for this peer
		if(this.role == ServerRole.Leader){
			this.requestAppendEntries(false);
		}
		
		// try to commit with this response
		this.tryToCommit(context, response.getNextIndex());
	}
	
	private void handleVotingResponse(RaftResponseMessage response, RequestContext context){
		this.votesResponded += 1;
		if(this.electionCompleted){
			this.logger.info("Election completed, will ignore the voting result from this server");
			return;
		}
		
		if(response.isAccepted()){
			this.votesGranted += 1;
		}
		
		if(this.votesResponded >= context.getTotalServers()){
			this.electionCompleted = true;
		}
		
		// got a majority set of granted votes
		if(this.votesGranted > context.getTotalServers() / 2){
			this.logger.info("Server is elected as leader for term %d", this.state.getTerm());
			this.electionCompleted = true;
			this.becomeLeader();
		}
	}
	
	private void handleHeartbeatTimeout(PeerServer peer){
		this.logger.debug("Heartbeat timeout for %d", peer.getId());
		synchronized(this){
			if(this.role == ServerRole.Leader){
				this.requestAppendEntries(false);
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
			server.enableHeartbeat(true);
			server.resumeHeartbeatingSpeed();
			server.setHeartbeatTask(this.scheduler.schedule(server.getHeartbeartHandler(), server.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
		}
		
		this.requestAppendEntries(true);
	}
	
	private void becomeFollower(){
		// stop heartbeat for all peers
		for(PeerServer server : this.peers.values()){
			server.enableHeartbeat(false);
			server.getHeartbeatTask().cancel(false);
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
			this.context.getServerStateManager().persistState(this.id, this.state);
			this.becomeFollower();
			return true;
		}
		
		return false;
	}
	
	private void commit(long targetIndex){
		if(targetIndex > this.commitIndex){
			while(this.commitIndex < targetIndex && this.commitIndex < this.logStore.getFirstAvailableIndex() - 1){
				long indexToCommit = this.commitIndex + 1;
				this.stateMachine.commit(indexToCommit, this.logStore.getLogEntryAt(indexToCommit).getValue());
				this.commitIndex = indexToCommit;
			}
			
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
			commitIndex = this.commitIndex;
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
	
	@SuppressWarnings("unchecked")
	private void tryToCommit(RequestContext context, long peerNextIndex){
		synchronized(context){
			if(context.getState() instanceof List<?>){
				List<Long> nextIndexes = (List<Long>)context.getState();
				nextIndexes.add(peerNextIndex == 0 ? 1 : peerNextIndex);
				
				// we have all responses, now try to commit new values
				if(nextIndexes.size() == context.getTotalServers()){
					nextIndexes.sort(new Comparator<Long>(){

						@Override
						public int compare(Long arg0, Long arg1) {
							return (int)(arg0 - arg1);
						}});
					this.commit(nextIndexes.get(context.getTotalServers() / 2) - 1);
				}
			}
		}
	}
}
