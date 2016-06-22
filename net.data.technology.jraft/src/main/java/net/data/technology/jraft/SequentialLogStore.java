package net.data.technology.jraft;

public interface SequentialLogStore {

    /**
     * The first available index of the store, starts with 1
     * @return value >= 1
     */
    public long getFirstAvailableIndex();

    /**
     * The start index of the log store, at the very beginning, it must be 1
     * however, after some compact actions, this could be anything greater or equals to one
     * @return
     */
    public long getStartIndex();

    /**
     * The last log entry in store
     * @return a dummy constant entry with value set to null and term set to zero if no log entry in store
     */
    public LogEntry getLastLogEntry();

    /**
     * Appends a log entry to store
     * @param logEntry
     */
    public void append(LogEntry logEntry);

    /**
     * Over writes a log entry at index of {@code index}
     * @param index a value < {@code this.getFirstAvailableIndex()}, and starts from 1
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
     * @param index, starts from 1
     * @return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
     */
    public LogEntry getLogEntryAt(long index);

    /**
     * Pack {@code itemsToPack} log items starts from {@code index}
     * @param index
     * @param itemsToPack
     * @return log pack
     */
    public byte[] packLog(long index, int itemsToPack);

    /**
     * Apply the log pack to current log store, starting from index
     * @param index, the log index that start applying the logPack, index starts from 1
     * @param logPack
     */
    public void applyLogPack(long index, byte[] logPack);

    /**
     * Compact the log store by removing all log entries including the log at the lastLogIndex
     * @param lastLogIndex
     * @return compact successfully or not
     */
    public boolean compact(long lastLogIndex);
}
