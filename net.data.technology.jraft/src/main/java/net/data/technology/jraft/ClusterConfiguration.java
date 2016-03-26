package net.data.technology.jraft;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ClusterConfiguration {

	private long logIndex;
	private long lastLogIndex;
	private List<ClusterServer> servers;
	
	public ClusterConfiguration(){
		this.servers = new LinkedList<ClusterServer>();
		this.logIndex = 0;
		this.lastLogIndex = 0;
	}
	
	public static ClusterConfiguration fromBytes(byte[] bytes){
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		ClusterConfiguration configuration = new ClusterConfiguration();
		configuration.setLogIndex(buffer.getLong());
		configuration.setLastLogIndex(buffer.getLong());
		while(buffer.hasRemaining()){
			int serverId = buffer.getInt();
			int size = buffer.getInt();
			byte[] endpointData = new byte[size];
			buffer.get(endpointData);
			ClusterServer server = new ClusterServer();
			server.setId(serverId);
			server.setEndpoint(new String(endpointData, StandardCharsets.UTF_8));
			configuration.getServers().add(server);
		}
		
		return configuration;
	}

	public long getLogIndex() {
		return logIndex;
	}

	public void setLogIndex(long logIndex) {
		this.logIndex = logIndex;
	}

	public long getLastLogIndex() {
		return lastLogIndex;
	}

	public void setLastLogIndex(long lastLogIndex) {
		this.lastLogIndex = lastLogIndex;
	}

	public List<ClusterServer> getServers() {
		return servers;
	}
	
	public ClusterServer getServer(int id){
		for(ClusterServer server : this.servers){
			if(server.getId() == id){
				return server;
			}
		}
		
		return null;
	}
	
	public byte[] toBytes(){
		int totalSize = Long.BYTES * 2;
		List<byte[]> serversData = new ArrayList<byte[]>(this.servers.size());
		for(int i = 0; i < this.servers.size(); ++i){
			ClusterServer server = this.servers.get(i);
			byte[] endpointData = server.getEndpoint().getBytes(StandardCharsets.UTF_8);
			ByteBuffer buffer = ByteBuffer.allocate(endpointData.length + 2 * Integer.BYTES);
			buffer.putInt(server.getId());
			buffer.putInt(endpointData.length);
			buffer.put(endpointData);
			serversData.add(buffer.array());
			totalSize += 2 * Integer.BYTES + endpointData.length;
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(totalSize);
		buffer.putLong(this.logIndex);
		buffer.putLong(this.lastLogIndex);
		for(int i = 0; i < serversData.size(); ++i){
			buffer.put(serversData.get(i));
		}
		
		return buffer.array();
	}
}
