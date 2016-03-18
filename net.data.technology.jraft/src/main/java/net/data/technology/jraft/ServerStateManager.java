package net.data.technology.jraft;

public interface ServerStateManager {

	public void persistState(int serverId, ServerState serverState);
	
	public ServerState readState(int serverId);
	
	public SequentialLogStore loadLogStore(int serverId);
}
