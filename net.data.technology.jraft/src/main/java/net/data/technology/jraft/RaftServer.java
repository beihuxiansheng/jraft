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
import java.util.concurrent.atomic.AtomicInteger;

public class RaftServer implements RaftMessageHandler {

	private static final int DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE = 4 * 1024;
	private static final Comparator<Long> indexComparator = new Comparator<Long>(){

		@Override
		public int compare(Long arg0, Long arg1) {
			return (int)(arg1.longValue() - arg0.longValue());
		}};
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
	private boolean configChanging = false;
	private boolean catchingUp = false;
	private int steppingDown = 0;
	private AtomicInteger snapshotInProgress;
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
		this.snapshotInProgress = new AtomicInteger(0);
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
		
		
		if(this.state == null){
			this.state = new ServerState();
			this.state.setTerm(0);
			this.state.setVotedFor(-1);
			this.state.setCommitIndex(0);
		}

		// try to load uncommitted, no need to check snapshots
		for(long i = this.state.getCommitIndex() + 1; i < this.logStore.getFirstAvailableIndex(); ++i){
			LogEntry logEntry = this.logStore.getLogEntryAt(i);
			if(logEntry.getValueType() == LogValueType.Configuration){
				this.config = ClusterConfiguration.fromBytes(logEntry.getValue());
				break;
			}
		}
		
		for(ClusterServer server : this.config.getServers()){
			if(server.getId() != this.id){
				this.peers.put(server.getId(), new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer)));
			}
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
		if(request.getMessageType() == RaftMessageType.AppendEntriesRequest 
			|| request.getMessageType() == RaftMessageType.RequestVoteRequest
			|| request.getMessageType() == RaftMessageType.InstallSnapshotRequest){
			// we allow the server to be continue after term updated to save a round message
			this.updateTerm(request.getTerm());
			
			// Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
			if(this.steppingDown > 0){
				this.steppingDown = 2;
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
		this.requestAppendEntries();
		response.setAccepted(true);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		return response;
	}

	private synchronized void handleElectionTimeout(){
		if(this.steppingDown > 0){
			if(--this.steppingDown == 0){
				this.logger.info("no hearing further news from leader, step down");
				System.exit(0);
				return;
			}
			
			this.logger.info("stepping down (cycles left: %d), skip this election timeout event", this.steppingDown);
			this.restartElectionTimer();
			return;
		}
		
		if(this.catchingUp){
			// this is a new server for the cluster, will not send out vote request until the config that includes this server is committed
			this.logger.info("election timeout while joining the cluster, ignore it.");
		}
		
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
	
	private void requestAppendEntries(){
		if(this.peers.size() == 0){
			this.commit(this.logStore.getFirstAvailableIndex() - 1);
			return;
		}
		
		for(PeerServer peer : this.peers.values()){
			this.requestAppendEntries(peer);
		}
	}
	
	private boolean requestAppendEntries(PeerServer peer){
		if(peer.makeBusy()){
			peer.SendRequest(this.createAppendEntriesRequest(peer))
				.whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
					handlePeerResponse(response, error);
				});
			return true;
		}
		
		this.logger.debug("Server %d is busy, skip the request", peer.getId());
		return false;
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
		}else if(response.getMessageType() == RaftMessageType.InstallSnapshotResponse){
			this.handleInstallSnapshotResponse(response);
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
		
		// If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
		boolean needToCatchup = true;
		if(response.isAccepted()){
			synchronized(peer){
				peer.setNextLogIndex(response.getNextIndex());
				peer.setMatchedIndex(response.getNextIndex() - 1);
			}
			
			// try to commit with this response
			ArrayList<Long> matchedIndexes = new ArrayList<Long>(this.peers.size() + 1);
			matchedIndexes.add(this.logStore.getFirstAvailableIndex() - 1);
			for(PeerServer p : this.peers.values()){
				matchedIndexes.add(p.getMatchedIndex());
			}
			
			matchedIndexes.sort(indexComparator);
			this.commit(matchedIndexes.get((this.peers.size() + 1) / 2));
			needToCatchup = peer.clearPendingCommit() || response.getNextIndex() < this.logStore.getFirstAvailableIndex();
		}else{
			synchronized(peer){
				peer.setNextLogIndex(peer.getNextLogIndex() - 1);
			}
		}
		
		peer.setFree();
		
		// This may not be a leader anymore, such as the response was sent out long time ago
        // and the role was updated by UpdateTerm call
        // Try to match up the logs for this peer
		if(this.role == ServerRole.Leader && needToCatchup){
			this.requestAppendEntries(peer);
		}
	}
	
	private void handleInstallSnapshotResponse(RaftResponseMessage response){
		PeerServer peer = this.peers.get(response.getSource());
		if(peer == null){
			this.logger.info("the response is from an unkonw peer %d", response.getSource());
			return;
		}
		
		// If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
		boolean needToCatchup = true;
		if(response.isAccepted()){
			synchronized(peer){
				SnapshotSyncContext context = peer.getSnapshotSyncContext();
				if(context == null){
					this.logger.info("no snapshot sync context for this peer, drop the response");
					needToCatchup = false;
				}else{
					if(response.getNextIndex() >= context.getSnapshot().getSize()){
						this.logger.debug("snapshot sync is done");
						peer.setNextLogIndex(context.getSnapshot().getLastLogIndex() + 1);
						peer.setMatchedIndex(context.getSnapshot().getLastLogIndex());
						peer.setSnapshotInSync(null);
						needToCatchup = peer.clearPendingCommit() || response.getNextIndex() < this.logStore.getFirstAvailableIndex();
					}else{
						this.logger.debug("continue to sync snapshot at offset %d", response.getNextIndex());
						context.setOffset(response.getNextIndex());
					}
				}
			}

		}else{
			this.logger.info("peer declines to install the snapshot, will retry");
		}
		
		peer.setFree();
		
		// This may not be a leader anymore, such as the response was sent out long time ago
        // and the role was updated by UpdateTerm call
        // Try to match up the logs for this peer
		if(this.role == ServerRole.Leader && needToCatchup){
			this.requestAppendEntries(peer);
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
			this.requestAppendEntries(peer);
			
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
		// don't start the election timer while this server is still catching up the logs
		if(this.catchingUp){
			return;
		}
		
		if(this.scheduledElection != null){
			this.scheduledElection.cancel(false);
		}

		RaftParameters parameters = this.context.getRaftParameters();
		int electionTimeout = parameters.getElectionTimeoutLowerBound() + this.random.nextInt(parameters.getElectionTimeoutUpperBound() - parameters.getElectionTimeoutLowerBound() + 1);
		this.scheduledElection = this.scheduler.schedule(this.electionTimeoutTask, electionTimeout, TimeUnit.MILLISECONDS);
	}
	
	private void stopElectionTimer(){
		if(this.scheduledElection == null){
			this.logger.warning("Election Timer is never started but is requested to stop, protential a bug");
			return;
		}
		
		this.scheduledElection.cancel(false);
		this.scheduledElection = null;
	}
	
	private void becomeLeader(){
		this.stopElectionTimer();
		this.role = ServerRole.Leader;
		this.leader = this.id;
		this.serverToJoin = null;
		for(PeerServer server : this.peers.values()){
			server.setNextLogIndex(this.logStore.getFirstAvailableIndex());
			server.setSnapshotInSync(null);
			this.enableHeartbeatForPeer(server);
		}
		
		// if current config is not committed, try to commit it
		if(this.config.getLogIndex() == 0){
			this.config.setLogIndex(this.logStore.getFirstAvailableIndex());
			this.logStore.append(new LogEntry(this.state.getTerm(), this.config.toBytes(), LogValueType.Configuration));
		}
		
		this.requestAppendEntries();
	}
	
	private void enableHeartbeatForPeer(PeerServer peer){
		peer.enableHeartbeat(true);
		peer.resumeHeartbeatingSpeed();
		peer.setHeartbeatTask(this.scheduler.schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
	}
	
	private void becomeFollower(){
		// stop heartbeat for all peers
		for(PeerServer server : this.peers.values()){
			if(server.getHeartbeatTask() != null){
				server.getHeartbeatTask().cancel(false);
			}

			server.enableHeartbeat(false);
		}
		
		this.serverToJoin = null;
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
			boolean snapshotInAction = false;
			try{
				while(this.state.getCommitIndex() < targetIndex && this.state.getCommitIndex() < this.logStore.getFirstAvailableIndex() - 1){
					long indexToCommit = this.state.getCommitIndex() + 1;
					LogEntry logEntry = this.logStore.getLogEntryAt(indexToCommit);
					if(logEntry.getValueType() == LogValueType.Application){
						this.stateMachine.commit(indexToCommit, logEntry.getValue());
					}else if(logEntry.getValueType() == LogValueType.Configuration){
						this.context.getServerStateManager().saveClusterConfiguration(this.config);
						if(this.catchingUp && this.config.getServer(this.id) != null){
							this.logger.info("this server is committed as one of cluster members");
							this.catchingUp = false;
						}
					}
					
					this.state.setCommitIndex(indexToCommit);
					
					// see if we need to do snapshots
					if(this.context.getRaftParameters().getSnapshotDistance() > 0 
						&& ((indexToCommit - this.logStore.getStartIndex()) > this.context.getRaftParameters().getSnapshotDistance())
						&& this.snapshotInProgress.compareAndSet(0, 1)){
						snapshotInAction = true;
						Snapshot currentSnapshot = this.stateMachine.getLastSnapshot();
						if(currentSnapshot != null && indexToCommit - currentSnapshot.getLastLogIndex() < this.context.getRaftParameters().getSnapshotDistance()){
							this.logger.info("a very recent snapshot is available at index %d, will skip this one", currentSnapshot.getLastLogIndex());
							this.snapshotInProgress.set(0);
							snapshotInAction = false;
						}else{
							this.logger.info("creating a snapshot for index %d", indexToCommit);
							
							// get the latest configuration info
							ClusterConfiguration config = this.config;
							while(config.getLogIndex() > indexToCommit && config.getLastLogIndex() >= this.logStore.getStartIndex()){
								config = ClusterConfiguration.fromBytes(this.logStore.getLogEntryAt(config.getLastLogIndex()).getValue());
							}
							
							if(config.getLogIndex() > indexToCommit && config.getLastLogIndex() > 0 && config.getLastLogIndex() < this.logStore.getStartIndex()){
								Snapshot lastSnapshot = this.stateMachine.getLastSnapshot();
								if(lastSnapshot == null){
									this.logger.error("No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting");
									System.exit(-1);
									return;
								}
								
								config = lastSnapshot.getLastConfig();
							}else if(config.getLogIndex() > indexToCommit && config.getLastLogIndex() == 0){
								this.logger.error("BUG!!! stop the system, there must be a configuration at index one");
								System.exit(-1);
							}
							
							long indexToCompact = indexToCommit - 1;
							long logTermToCompact = this.logStore.getLogEntryAt(indexToCompact).getTerm();
							Snapshot snapshot = new Snapshot(indexToCompact, logTermToCompact, config);
							this.stateMachine.createSnapshot(snapshot).whenCompleteAsync((Boolean result, Throwable error) -> {
								try{
									if(error != null){
										this.logger.error("failed to create a snapshot due to %s", error.getMessage());
										return;
									}
									
									if(!result.booleanValue()){
										this.logger.info("the state machine rejects to create the snapshot");
										return;
									}
									
									synchronized(this){
										this.logger.debug("snapshot created, compact the log store");
										try{
											this.logStore.compact(snapshot.getLastLogIndex());
										}catch(Throwable ex){
											this.logger.error("failed to compact the log store, no worries, the system still in a good shape", ex);
										}
									}
								}finally{
									this.snapshotInProgress.set(0);
								}
							});
							snapshotInAction = false;
						}
					}
				}
			}catch(Throwable error){
				this.logger.error("failed to commit to index %d, due to errors %s", targetIndex, error.toString());
				if(snapshotInAction){
					this.snapshotInProgress.compareAndSet(1, 0);
				}
			}
			
			// save the commitment state
			this.context.getServerStateManager().persistState(this.state);
			
			// if this is a leader notify peers to commit as well
			// for peers that are free, send the request, otherwise, set pending commit flag for that peer
			if(this.role == ServerRole.Leader){
				for(PeerServer peer : this.peers.values()){
					if(!this.requestAppendEntries(peer)){
						peer.setPendingCommit();
					}
				}
			}
		}
	}
	
	private RaftRequestMessage createAppendEntriesRequest(PeerServer peer){
		long currentNextIndex = 0;
		long commitIndex = 0;
		long lastLogIndex = 0;
		long term = 0;
		long startingIndex = 1;
		
		synchronized(this){
			startingIndex = this.logStore.getStartIndex();
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
		
		// for syncing the snapshots
		if(lastLogIndex > 0 && lastLogIndex < startingIndex){
			return this.createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex);
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
			if(peer.getHeartbeatTask() != null){
				peer.getHeartbeatTask().cancel(false);
			}

			peer.enableHeartbeat(false);
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
			return this.handleLeaveClusterRequest(request);
		}else if(request.getMessageType() == RaftMessageType.InstallSnapshotRequest){
			return this.handleInstallSnapshotRequest(request);
		}else{
			this.logger.error("receive an unknown request %s, for safety, step down.", request.getMessageType().toString());
			System.exit(-1);
		}
		
		return null;
	}
	
	private RaftResponseMessage handleInstallSnapshotRequest(RaftRequestMessage request){
		if(request.getTerm() == this.state.getTerm() && !this.catchingUp){
			if(this.role == ServerRole.Candidate){
				this.becomeFollower();
			}else if(this.role == ServerRole.Leader){
				this.logger.error("Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a bug, server exits", request.getSource());
				System.exit(-1);
			}else{
				this.restartElectionTimer();
			}
		}
		
		RaftResponseMessage response = new RaftResponseMessage();
		response.setMessageType(RaftMessageType.InstallSnapshotResponse);
		response.setTerm(this.state.getTerm());
		response.setSource(this.id);
		response.setDestination(request.getSource());
		if(!this.catchingUp && request.getTerm() < this.state.getTerm()){
			this.logger.info("received an install snapshot request which has lower term than this server, decline the request");
			response.setAccepted(false);
			response.setNextIndex(0);
			return response;
		}
		
		LogEntry logEntries[] = request.getLogEntries();
		if(logEntries == null || logEntries.length != 1 || logEntries[0].getValueType() != LogValueType.SnapshotSyncRequest){
			this.logger.warning("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value");
			response.setNextIndex(0);
			response.setAccepted(false);
			return response;
		}
		
		SnapshotSyncRequest snapshotSyncRequest = SnapshotSyncRequest.fromBytes(logEntries[0].getValue());
		response.setAccepted(this.handleSnapshotSyncRequest(snapshotSyncRequest));
		response.setNextIndex(snapshotSyncRequest.getOffset() + snapshotSyncRequest.getData().length);
		return response;
	}
	
	private boolean handleSnapshotSyncRequest(SnapshotSyncRequest snapshotSyncRequest){
		try{
			this.stateMachine.saveSnapshotData(snapshotSyncRequest.getSnapshot(), snapshotSyncRequest.getOffset(), snapshotSyncRequest.getData());
			if(snapshotSyncRequest.isDone()){
				this.logger.debug("sucessfully receive a snapshot from leader");
				if(this.logStore.compact(snapshotSyncRequest.getSnapshot().getLastLogIndex())){
					this.logger.info("successfully compact the log store, will now ask the statemachine to apply the snapshot");
					if(!this.stateMachine.applySnapshot(snapshotSyncRequest.getSnapshot())){
						this.logger.error("failed to apply the snapshot after log compacted, to ensure the safety, will shutdown the system");
						System.exit(-1);
						return false; //should never be reached
					}
					
					this.reconfigure(snapshotSyncRequest.getSnapshot().getLastConfig());
					this.context.getServerStateManager().saveClusterConfiguration(this.config);
					this.state.setCommitIndex(snapshotSyncRequest.getSnapshot().getLastLogIndex());
					this.context.getServerStateManager().persistState(this.state);
					this.logger.info("snapshot is successfully applied");
				}else{
					this.logger.error("failed to compact the log store after a snapshot is received, will ask the leader to retry");
					return false;
				}
			}
		}catch(Throwable error){
			this.logger.error("I/O error %s while saving the snapshot or applying the snapshot, no reason to continue", error.getMessage());
			System.exit(-1);
			return false;
		}
		
		return true;
	}
	
	private synchronized void handleExtendedResponse(RaftResponseMessage response, Throwable error){
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
				this.serverToJoin.setNextLogIndex(response.getNextIndex());
				this.serverToJoin.setMatchedIndex(response.getNextIndex() - 1);
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
		}else if(response.getMessageType() == RaftMessageType.LeaveClusterResponse){
			if(!response.isAccepted()){
				this.logger.info("peer doesn't accept to stepping down, stop proceeding");
				return;
			}
			
			this.logger.debug("peer accepted to stepping down, removing this server from cluster");
			this.removeServerFromCluster(response.getSource());
		}else if(response.getMessageType() == RaftMessageType.InstallSnapshotResponse){
			if(this.serverToJoin == null){
				this.logger.info("no server to join, the response must be very old.");
				return;
			}
			
			if(!response.isAccepted()){
				this.logger.info("peer doesn't accept the snapshot installation request");
				return;
			}
			
			SnapshotSyncContext context = this.serverToJoin.getSnapshotSyncContext();
			if(context == null){
				this.logger.error("Bug! SnapshotSyncContext must not be null");
				System.exit(-1);
				return;
			}
			
			if(response.getNextIndex() >= context.getSnapshot().getSize()){
				// snapshot is done
				this.logger.debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
				this.serverToJoin.setSnapshotInSync(null);
				this.serverToJoin.setNextLogIndex(context.getSnapshot().getLastLogIndex() + 1);
				this.serverToJoin.setMatchedIndex(response.getNextIndex() - 1);
				this.syncLogsToNewComingServer(this.serverToJoin.getNextLogIndex());
			}else{
				context.setOffset(response.getNextIndex());
				this.logger.debug("continue to send snapshot to new server at offset %d", response.getNextIndex());
				this.syncLogsToNewComingServer(this.serverToJoin.getNextLogIndex());
			}
		}else{
			// No more response message types need to be handled
			this.logger.error("received an unexpected response message type %s, for safety, stepping down", response.getMessageType());
			System.exit(-1);
		}
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
			if(request.getMessageType() == RaftMessageType.SyncLogRequest || request.getMessageType() == RaftMessageType.JoinClusterRequest || request.getMessageType() == RaftMessageType.LeaveClusterRequest){
				final PeerServer server = (request.getMessageType() == RaftMessageType.LeaveClusterRequest) ? this.peers.get(request.getDestination()) : this.serverToJoin;
				if(server != null){
					if(server.getCurrentHeartbeatInterval() >= this.context.getRaftParameters().getMaxHeartbeatInterval()){
						if(request.getMessageType() == RaftMessageType.LeaveClusterRequest){
							this.logger.info("rpc failed again for the removing server (%d), will remove this server directly", server.getId());
							this.removeServerFromCluster(server.getId());
						}else{
							this.logger.info("rpc failed again for the new coming server (%d), will stop retry for this server", server.getId());
							this.configChanging = false;
							this.serverToJoin = null;
						}
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
		
		if(this.configChanging || this.config.getLogIndex() == 0 || this.config.getLogIndex() > this.state.getCommitIndex()){
			// the previous config has not committed yet
			this.logger.info("previous config has not committed yet");
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
		
		this.configChanging = true;
		RaftRequestMessage leaveClusterRequest = new RaftRequestMessage();
		leaveClusterRequest.setCommitIndex(this.state.getCommitIndex());
		leaveClusterRequest.setDestination(peer.getId());
		leaveClusterRequest.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
		leaveClusterRequest.setLastLogTerm(0);
		leaveClusterRequest.setTerm(this.state.getTerm());
		leaveClusterRequest.setMessageType(RaftMessageType.LeaveClusterRequest);
		leaveClusterRequest.setSource(this.id);
		peer.SendRequest(leaveClusterRequest).whenCompleteAsync((RaftResponseMessage peerResponse, Throwable error) -> {
			this.handleExtendedResponse(peerResponse, error);
		});
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
		
		if(this.configChanging || this.config.getLogIndex() == 0 || this.config.getLogIndex() > this.state.getCommitIndex()){
			// the previous config has not committed yet
			this.logger.info("previous config has not committed yet");
			return response;
		}
		
		this.configChanging = true;
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
			this.configChanging = false;
			this.requestAppendEntries();
			return;
		}
		
		RaftRequestMessage request = null;
		if(startIndex > 0 && startIndex < this.logStore.getStartIndex()){
			request = this.createSyncSnapshotRequest(this.serverToJoin, startIndex, this.state.getTerm(), this.state.getCommitIndex());
			
		}else{
			int sizeToSync = Math.min(gap, this.context.getRaftParameters().getLogSyncBatchSize());
			byte[] logPack = this.logStore.packLog(startIndex, sizeToSync);
			request = new RaftRequestMessage();
			request.setCommitIndex(this.state.getCommitIndex());
			request.setDestination(this.serverToJoin.getId());
			request.setSource(this.id);
			request.setTerm(this.state.getTerm());
			request.setMessageType(RaftMessageType.SyncLogRequest);
			request.setLastLogIndex(startIndex - 1);
			request.setLogEntries(new LogEntry[] { new LogEntry(this.state.getTerm(), logPack, LogValueType.LogPack) });
		}
		
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
		this.state.setTerm(request.getTerm());
		this.state.setCommitIndex(0);
		this.state.setVotedFor(-1);
		this.context.getServerStateManager().persistState(this.state);
		this.stopElectionTimer();
		ClusterConfiguration newConfig = ClusterConfiguration.fromBytes(logEntries[0].getValue());
		this.reconfigure(newConfig);
		response.setTerm(this.state.getTerm());
		response.setAccepted(true);
		return response;
	}
	
	private RaftResponseMessage handleLeaveClusterRequest(RaftRequestMessage request){
		this.steppingDown = 2;
		RaftResponseMessage response = new RaftResponseMessage();
		response.setSource(this.id);
		response.setDestination(request.getSource());
		response.setTerm(this.state.getTerm());
		response.setMessageType(RaftMessageType.LeaveClusterResponse);
		response.setNextIndex(this.logStore.getFirstAvailableIndex());
		response.setAccepted(true);
		return response;
	}
	
	private void removeServerFromCluster(int serverId){
		PeerServer peer = this.peers.get(serverId);
		if(peer.getHeartbeatTask() != null){
			peer.getHeartbeatTask().cancel(false);
		}
		
		peer.enableHeartbeat(false);
		this.peers.remove(serverId);
		ClusterConfiguration newConfig = new ClusterConfiguration();
		newConfig.setLastLogIndex(this.config.getLogIndex());
		newConfig.setLogIndex(this.logStore.getFirstAvailableIndex());
		for(ClusterServer server: this.config.getServers()){
			if(server.getId() != serverId){
				newConfig.getServers().add(server);
			}
		}
		
		this.configChanging = false;
		this.logStore.append(new LogEntry(this.state.getTerm(), newConfig.toBytes(), LogValueType.Configuration));
		this.config = newConfig;
		this.requestAppendEntries();
	}
	
	private int getSnapshotSyncBlockSize(){
		int blockSize = this.context.getRaftParameters().getSnapshotBlockSize();
		return blockSize == 0 ? DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE : blockSize;
	}
	
	private RaftRequestMessage createSyncSnapshotRequest(PeerServer peer, long lastLogIndex, long term, long commitIndex){
		synchronized(peer){
			SnapshotSyncContext context = peer.getSnapshotSyncContext();
			Snapshot snapshot = context == null ? null : context.getSnapshot();
			Snapshot lastSnapshot = this.stateMachine.getLastSnapshot();
			if(snapshot == null || (lastSnapshot != null && lastSnapshot.getLastLogIndex() > snapshot.getLastLogIndex())){
				snapshot = this.stateMachine.getLastSnapshot();
				
				if(snapshot == null || lastLogIndex > snapshot.getLastLogIndex()){
					this.logger.error("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot null: %s, snapshot doesn't contais lastLogIndex: %s)", peer.getId(), String.valueOf(snapshot == null), String.valueOf(lastLogIndex > snapshot.getLastLogIndex()));
					System.exit(-1);
					return null;
				}
				
				if(snapshot.getSize() < 1L){
					this.logger.error("invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors");
					System.exit(-1);
					return null;
				}
				
				this.logger.info("trying to sync snapshot with last index %d to peer %d", snapshot.getLastLogIndex(), peer.getId());
				peer.setSnapshotInSync(snapshot);
			}
			
			long offset = peer.getSnapshotSyncContext().getOffset();
			long sizeLeft = snapshot.getSize() - offset;
			int blockSize = this.getSnapshotSyncBlockSize();
			byte[] data = new byte[sizeLeft > blockSize ? blockSize : (int)sizeLeft];
			try{
				int sizeRead = this.stateMachine.readSnapshotData(snapshot, offset, data);
				if(sizeRead < data.length){
					this.logger.error("only %d bytes could be read from snapshot while %d bytes are expected, should be something wrong" , sizeRead, data.length);
					System.exit(-1);
					return null;
				}
			}catch(Throwable error){
				// if there is i/o error, no reason to continue
				this.logger.error("failed to read snapshot data due to io error %s", error.toString());
				System.exit(-1);
				return null;
			}
			SnapshotSyncRequest syncRequest = new SnapshotSyncRequest(snapshot, offset, data, (offset + data.length) >= snapshot.getSize());
			RaftRequestMessage requestMessage = new RaftRequestMessage();
			requestMessage.setMessageType(RaftMessageType.InstallSnapshotRequest);
			requestMessage.setSource(this.id);
			requestMessage.setDestination(peer.getId());
			requestMessage.setLastLogIndex(snapshot.getLastLogIndex());
			requestMessage.setLastLogTerm(snapshot.getLastLogTerm());
			requestMessage.setLogEntries(new LogEntry[] { new LogEntry(term, syncRequest.toBytes(), LogValueType.SnapshotSyncRequest) });
			requestMessage.setCommitIndex(commitIndex);
			requestMessage.setTerm(term);
			return requestMessage;
		}
	}
}
