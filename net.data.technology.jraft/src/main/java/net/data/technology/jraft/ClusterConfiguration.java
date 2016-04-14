package net.data.technology.jraft;

import java.nio.ByteBuffer;
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
	
	public static ClusterConfiguration fromBytes(ByteBuffer buffer){
		ClusterConfiguration configuration = new ClusterConfiguration();
		configuration.setLogIndex(buffer.getLong());
		configuration.setLastLogIndex(buffer.getLong());
		while(buffer.hasRemaining()){
			configuration.getServers().add(new ClusterServer(buffer));
		}
		
		return configuration;
	}
	
	public static ClusterConfiguration fromBytes(byte[] data){
		return fromBytes(ByteBuffer.wrap(data));
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
			byte[] dataForServer = server.toBytes();
			totalSize += dataForServer.length;
			serversData.add(dataForServer);
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
