package net.data.technology.jraft;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ClusterServer {

	private int id;
	private String endpoint;
	
	public ClusterServer(){
		this.id = -1;
		this.endpoint = null;
	}
	
	public ClusterServer(ByteBuffer buffer){
		this.id = buffer.getInt();
		int dataSize = buffer.getInt();
		byte[] endpointData = new byte[dataSize];
		buffer.get(endpointData);
		this.endpoint = new String(endpointData, StandardCharsets.UTF_8);
	}
	
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
	
	public byte[] toBytes(){
		byte[] endpointData = this.endpoint.getBytes(StandardCharsets.UTF_8);
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + endpointData.length);
		buffer.putInt(this.id);
		buffer.putInt(endpointData.length);
		buffer.put(endpointData);
		return buffer.array();
	}
}
