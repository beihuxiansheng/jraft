package net.data.technology.jraft;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class RaftContext {

    private ServerStateManager serverStateManager;
    private RpcListener rpcListener;
    private LoggerFactory loggerFactory;
    private RpcClientFactory rpcClientFactory;
    private StateMachine stateMachine;
    private RaftParameters raftParameters;
    private ScheduledThreadPoolExecutor scheduledExecutor;

    public RaftContext(ServerStateManager stateManager, StateMachine stateMachine, RaftParameters raftParameters, RpcListener rpcListener, LoggerFactory logFactory, RpcClientFactory rpcClientFactory){
        this(stateManager, stateMachine, raftParameters, rpcListener, logFactory, rpcClientFactory, null);
    }

    public RaftContext(ServerStateManager stateManager, StateMachine stateMachine, RaftParameters raftParameters, RpcListener rpcListener, LoggerFactory logFactory, RpcClientFactory rpcClientFactory, ScheduledThreadPoolExecutor scheduledExecutor){
        this.serverStateManager = stateManager;
        this.stateMachine = stateMachine;
        this.raftParameters = raftParameters;
        this.rpcClientFactory = rpcClientFactory;
        this.rpcListener = rpcListener;
        this.loggerFactory = logFactory;
        this.scheduledExecutor = scheduledExecutor;
        if(this.scheduledExecutor == null){
            this.scheduledExecutor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
        }

        if(this.raftParameters == null){
            this.raftParameters = new RaftParameters()
                    .withElectionTimeoutUpper(300)
                    .withElectionTimeoutLower(150)
                    .withHeartbeatInterval(75)
                    .withRpcFailureBackoff(25)
                    .withMaximumAppendingSize(100)
                    .withLogSyncBatchSize(1000)
                    .withLogSyncStoppingGap(100)
                    .withSnapshotEnabled(0)
                    .withSyncSnapshotBlockSize(0);
        }
    }

    public ServerStateManager getServerStateManager() {
        return serverStateManager;
    }

    public RpcListener getRpcListener() {
        return rpcListener;
    }

    public LoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    public RpcClientFactory getRpcClientFactory() {
        return rpcClientFactory;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftParameters getRaftParameters() {
        return raftParameters;
    }

    public ScheduledThreadPoolExecutor getScheduledExecutor(){
        return this.scheduledExecutor;
    }
}
