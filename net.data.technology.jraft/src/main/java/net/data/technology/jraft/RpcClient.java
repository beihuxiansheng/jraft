package net.data.technology.jraft;

import java.util.concurrent.CompletableFuture;

public interface RpcClient {

	public CompletableFuture<RaftResponseMessage> send(RaftRequestMessage request);
}
