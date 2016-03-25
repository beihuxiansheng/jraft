package net.data.technology.jraft;

public class ClusterServer {

	private int id;
	private String endpoint;
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
}
