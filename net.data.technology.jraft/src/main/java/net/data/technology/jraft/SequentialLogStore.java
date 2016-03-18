package net.data.technology.jraft;

public interface SequentialLogStore {

	/**
	 * The first available index of the store, starts with one
	 * @return value >= 1
	 */
	public long getFirstAvailableIndex();
	
	/**
	 * The last log entry in store
	 * @return null if no log entry in store
	 */
	public LogEntry getLastLogEntry();
	
	/**
	 * Appends a log entry to store 
	 * @param logEntry
	 */
	public void append(LogEntry logEntry);
	
	/**
	 * Over writes a log entry at index of {@code index}
	 * @param index a value < {@code this.getFirstAvailableIndex()}
	 * @param logEntry
	 */
	public void writeAt(long index, LogEntry logEntry);
	
	/**
	 * Get log entries with index between {@code start} and {@code end}
	 * @param start, the start index of log entries
	 * @param end, the end index of log entries (exclusive)
	 * @return the log entries between [start, end)
	 */
	public LogEntry[] getLogEntries(long start, long end);
	
	/**
	 * Gets the log entry at the specified index
	 * @param index
	 * @return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
	 */
	public LogEntry getLogEntryAt(long index);
}
