package net.data.technology.jraft;

public class LogEntry {

	private byte[] value;
	private long term;
	private LogValueType vaueType;
	
	public LogEntry(){
		this(0, null);
	}
	
	public LogEntry(long term, byte[] value){
		this(term, value, LogValueType.Application);
	}
	
	public LogEntry(long term, byte[] value, LogValueType valueType){
		this.term = term;
		this.value = value;
		this.vaueType = valueType;
	}
	
	public long getTerm(){
		return this.term;
	}
	
	public byte[] getValue(){
		return this.value;
	}
	
	public LogValueType getValueType(){
		return this.vaueType;
	}
}
