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
	private AtomicInteger ongoingAppendRequests;
	private Callable<Void> heartbeatTimeoutHandler;
	private ScheduledFuture<?> heartbeatTask;
	private long nextLogIndex;
	private long matchedIndex;
	private boolean heartbeatEnabled;
	
	public PeerServer(ClusterServer server, RaftContext context, final Consumer<PeerServer> heartbeatConsumer){
		this.clusterConfig = server;
		this.rpcClient = context.getRpcClientFactory().createRpcClient(server.getEndpoint());
		this.ongoingAppendRequests = new AtomicInteger(0);
		this.heartbeatInterval = this.currentHeartbeatInterval = context.getRaftParameters().getHeartbeatInterval();
		this.maxHeartbeatInterval = context.getRaftParameters().getMaxHeartbeatInterval();
		this.rpcBackoffInterval = context.getRaftParameters().getRpcFailureBackoff();
		this.heartbeatTask = null;
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
	
	public boolean isBusy(){
		return this.ongoingAppendRequests.get() > 0;
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

	public CompletableFuture<RaftResponseMessage> SendRequest(RaftRequestMessage request){
		final boolean isAppendRequest = request.getMessageType() == RaftMessageType.AppendEntriesRequest;
		if(isAppendRequest){
			this.ongoingAppendRequests.incrementAndGet();
		}
		
		return this.rpcClient.send(request)
				.thenComposeAsync((RaftResponseMessage response) -> {
					if(isAppendRequest){
						this.ongoingAppendRequests.decrementAndGet();
					}
					
					this.resumeHeartbeatingSpeed();
					return CompletableFuture.completedFuture(response);
				})
				.exceptionally((Throwable error) -> {
					if(isAppendRequest){
						this.ongoingAppendRequests.decrementAndGet();
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
