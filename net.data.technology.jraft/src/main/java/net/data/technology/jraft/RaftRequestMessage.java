package net.data.technology.jraft;

import java.util.Arrays;

public class RaftRequestMessage extends RaftMessage {

    private long lastLogTerm;
    private long lastLogIndex;
    private long commitIndex;
    private LogEntry[] logEntries;

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public LogEntry[] getLogEntries() {
        return logEntries;
    }

    public void setLogEntries(LogEntry[] logEntries) {
        this.logEntries = logEntries;
    }

	@Override
	public String toString() {
		return "RaftRequestMessage [lastLogTerm=" + lastLogTerm + ", lastLogIndex=" + lastLogIndex + ", commitIndex="
				+ commitIndex + ", logEntries=" + Arrays.toString(logEntries) + "]";
	}
    
}
