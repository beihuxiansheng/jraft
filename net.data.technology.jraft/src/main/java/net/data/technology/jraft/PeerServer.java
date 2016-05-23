package net.data.technology.jraft;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class PeerServer {

	private ClusterServer clusterConfig;
	private RpcClient rpcClient;
	private int currentHeartbeatInterval;
	private int heartbeatInterval;
	private int rpcBackoffInterval;
	private int maxHeartbeatInterval;
	private AtomicInteger busyFlag;
	private AtomicInteger pendingCommitFlag;
	private Callable<Void> heartbeatTimeoutHandler;
	private ScheduledFuture<?> heartbeatTask;
	private long nextLogIndex;
	private long matchedIndex;
	private boolean heartbeatEnabled;
	private SnapshotSyncContext snapshotSyncContext;
	
	public PeerServer(ClusterServer server, RaftContext context, final Consumer<PeerServer> heartbeatConsumer){
		this.clusterConfig = server;
		this.rpcClient = context.getRpcClientFactory().createRpcClient(server.getEndpoint());
		this.busyFlag = new AtomicInteger(0);
		this.pendingCommitFlag = new AtomicInteger(0);
		this.heartbeatInterval = this.currentHeartbeatInterval = context.getRaftParameters().getHeartbeatInterval();
		this.maxHeartbeatInterval = context.getRaftParameters().getMaxHeartbeatInterval();
		this.rpcBackoffInterval = context.getRaftParameters().getRpcFailureBackoff();
		this.heartbeatTask = null;
		this.snapshotSyncContext = null;
		this.nextLogIndex = 1;
		this.matchedIndex = 0;
		this.heartbeatEnabled = false;
		PeerServer self = this;
		this.heartbeatTimeoutHandler = new Callable<Void>(){

			@Override
			public Void call() throws Exception {
				heartbeatConsumer.accept(self);
				return null;
			}};
	}
	
	public int getId(){
		return this.clusterConfig.getId();
	}
	
	public ClusterServer getClusterConfig(){
		return this.clusterConfig;
	}
	
	public Callable<Void> getHeartbeartHandler(){
		return this.heartbeatTimeoutHandler;
	}
	
	public synchronized int getCurrentHeartbeatInterval(){
		return this.currentHeartbeatInterval;
	}
	
	public void setHeartbeatTask(ScheduledFuture<?> heartbeatTask){
		this.heartbeatTask = heartbeatTask;
	}
	
	public ScheduledFuture<?> getHeartbeatTask(){
		return this.heartbeatTask;
	}
	
	public boolean makeBusy(){
		return this.busyFlag.compareAndSet(0, 1);
	}
	
	public void setFree(){
		this.busyFlag.set(0);
	}
	
	public boolean isHeartbeatEnabled(){
		return this.heartbeatEnabled;
	}
	
	public void enableHeartbeat(boolean enable){
		this.heartbeatEnabled = enable;
		
		if(!enable){
			this.heartbeatTask = null;
		}
	}
	
	public long getNextLogIndex() {
		return nextLogIndex;
	}

	public void setNextLogIndex(long nextLogIndex) {
		this.nextLogIndex = nextLogIndex;
	}
	
	public long getMatchedIndex(){
		return this.matchedIndex;
	}
	
	public void setMatchedIndex(long matchedIndex){
		this.matchedIndex = matchedIndex;
	}
	
	public void setPendingCommit(){
		this.pendingCommitFlag.set(1);
	}
	
	public boolean clearPendingCommit(){
		return this.pendingCommitFlag.compareAndSet(1, 0);
	}
	
	public void setSnapshotInSync(Snapshot snapshot){
		if(snapshot == null){
			this.snapshotSyncContext = null;
		}else{
			this.snapshotSyncContext = new SnapshotSyncContext(snapshot);
		}
	}
	
	public SnapshotSyncContext getSnapshotSyncContext(){
		return this.snapshotSyncContext;
	}

	public CompletableFuture<RaftResponseMessage> SendRequest(RaftRequestMessage request){
		boolean isAppendRequest = request.getMessageType() == RaftMessageType.AppendEntriesRequest || request.getMessageType() == RaftMessageType.InstallSnapshotRequest;
		return this.rpcClient.send(request)
				.thenComposeAsync((RaftResponseMessage response) -> {
					if(isAppendRequest){
						this.setFree();
					}

					this.resumeHeartbeatingSpeed();
					return CompletableFuture.completedFuture(response);
				})
				.exceptionally((Throwable error) -> {
					if(isAppendRequest){
						this.setFree();
					}
					
					this.slowDownHeartbeating();
					throw new RpcException(error, request);
				});
	}
	
	public synchronized void slowDownHeartbeating(){
		this.currentHeartbeatInterval = Math.min(this.maxHeartbeatInterval, this.currentHeartbeatInterval + this.rpcBackoffInterval);
	}
	
	public synchronized void resumeHeartbeatingSpeed(){
		if(this.currentHeartbeatInterval > this.heartbeatInterval){
			this.currentHeartbeatInterval = this.heartbeatInterval;
		}
	}
}
