package net.data.technology.jraft;

public interface RpcClientFactory {

	RpcClient createRpcClient(String endpoint);
}
