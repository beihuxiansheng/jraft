package net.data.technology.jraft;

public interface ServerStateManager {

    public ClusterConfiguration loadClusterConfiguration();

    public void saveClusterConfiguration(ClusterConfiguration configuration);

    public void persistState(ServerState serverState);

    public ServerState readState();

    public SequentialLogStore loadLogStore();

    public int getServerId();
}
