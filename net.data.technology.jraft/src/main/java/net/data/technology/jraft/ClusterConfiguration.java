package net.data.technology.jraft;

import java.util.LinkedList;
import java.util.List;

public class ClusterConfiguration {

	private int localServerId;
	private List<ClusterServer> servers;
	
	public ClusterConfiguration(){
		this.servers = new LinkedList<ClusterServer>();
	}

	public int getLocalServerId() {
		return localServerId;
	}

	public void setLocalServerId(int localServerId) {
		this.localServerId = localServerId;
	}

	public List<ClusterServer> getServers() {
		return servers;
	}
	
	public ClusterServer getLocalServer(){
		for(ClusterServer server : this.servers){
			if(server.getId() == this.localServerId){
				return server;
			}
		}
		
		return null;
	}
}
