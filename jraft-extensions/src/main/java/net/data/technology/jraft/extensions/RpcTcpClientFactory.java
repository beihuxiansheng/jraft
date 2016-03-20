package net.data.technology.jraft.extensions;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.RpcClient;
import net.data.technology.jraft.RpcClientFactory;

public class RpcTcpClientFactory implements RpcClientFactory {

	@Override
	public RpcClient createRpcClient(String endpoint) {
		try {
			URI uri = new URI(endpoint);
			return new RpcTcpClient(new InetSocketAddress(uri.getHost(), uri.getPort()));
		} catch (URISyntaxException e) {
			LogManager.getLogger(getClass()).error(String.format("%s is not a valid uri", endpoint));
			throw new IllegalArgumentException("invalid uri for endpoint");
		}
	}

}
