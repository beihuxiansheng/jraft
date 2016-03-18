package net.data.technology.jraft;

public class LogEntry {

	private byte[] value;
	private long term;
	
	public LogEntry(){
		this(0, null);
	}
	
	public LogEntry(long term, byte[] value){
		this.term = term;
		this.value = value;
	}
	
	public long getTerm(){
		return this.term;
	}
	
	public byte[] getValue(){
		return this.value;
	}
}
