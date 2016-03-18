package net.data.technology.jraft;

public class RaftMessage {

	private RaftMessageType messageType;
	private int source;
	private int destination;
	private long term;
	
	public RaftMessageType getMessageType() {
		return messageType;
	}
	
	public void setMessageType(RaftMessageType messageType) {
		this.messageType = messageType;
	}
	
	public int getSource() {
		return source;
	}
	
	public void setSource(int source) {
		this.source = source;
	}
	
	public int getDestination() {
		return destination;
	}
	
	public void setDestination(int destination) {
		this.destination = destination;
	}
	
	public long getTerm() {
		return term;
	}
	
	public void setTerm(long term) {
		this.term = term;
	}
}
