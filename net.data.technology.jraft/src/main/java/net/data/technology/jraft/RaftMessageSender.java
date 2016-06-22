package net.data.technology.jraft;

import java.util.concurrent.CompletableFuture;

public interface RaftMessageSender {

    /**
     * Add a new server to the cluster
     * @param server
     * @return true if request is accepted, or false if no leader, rpc fails or leader declines
     */
    CompletableFuture<Boolean> addServer(ClusterServer server);

    /**
     * Remove a server from cluster, the server will step down when the removal is confirmed
     * @param serverId, the id for the server to be removed
     * @return true if request is accepted or false if no leader, rpc fails or leader declines
     */
    CompletableFuture<Boolean> removeServer(int serverId);

    /**
     * Append multiple application logs to log store
     * @param values, the application log entries
     * @return true if request is accepted or false if no leader, rpc fails or leader declines
     */
    CompletableFuture<Boolean> appendEntries(byte[][] values);
}
