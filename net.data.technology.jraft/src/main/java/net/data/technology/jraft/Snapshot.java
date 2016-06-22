package net.data.technology.jraft;

public class Snapshot {

    private long lastLogIndex;
    private long lastLogTerm;
    private long size;
    private ClusterConfiguration lastConfig;

    public Snapshot(long lastLogIndex, long lastLogTerm, ClusterConfiguration lastConfig){
        this(lastLogIndex, lastLogTerm, lastConfig, 0);
    }

    public Snapshot(long lastLogIndex, long lastLogTerm, ClusterConfiguration lastConfig, long size){
        this.lastConfig = lastConfig;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.size = size;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public ClusterConfiguration getLastConfig() {
        return lastConfig;
    }

    public long getSize(){
        return this.size;
    }
}
