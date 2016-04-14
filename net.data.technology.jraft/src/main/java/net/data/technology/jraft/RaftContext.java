package net.data.technology.jraft;

public class RaftContext {

	private ServerStateManager serverStateManager;
	private RpcListener rpcListener;
	private LoggerFactory loggerFactory;
	private RpcClientFactory rpcClientFactory;
	private StateMachine stateMachine;
	private RaftParameters raftParameters;
	
	public RaftContext(ServerStateManager stateManager, StateMachine stateMachine, RaftParameters raftParameters, RpcListener rpcListener, LoggerFactory logFactory, RpcClientFactory rpcClientFactory){
		this.serverStateManager = stateManager;
		this.stateMachine = stateMachine;
		this.raftParameters = raftParameters;
		this.rpcClientFactory = rpcClientFactory;
		this.rpcListener = rpcListener;
		this.loggerFactory = logFactory;
		
		if(this.raftParameters == null){
			this.raftParameters = new RaftParameters(300, 150, 75, 25, 1000, 100, 0, 0);
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
}
