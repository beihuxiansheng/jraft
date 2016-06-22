package net.data.technology.jraft.extensions.http;

import net.data.technology.jraft.RpcClient;
import net.data.technology.jraft.RpcClientFactory;

public class HttpRpcClientFactory implements RpcClientFactory {

    @Override
    public RpcClient createRpcClient(String endpoint) {
        return new HttpRpcClient(endpoint);
    }

}
