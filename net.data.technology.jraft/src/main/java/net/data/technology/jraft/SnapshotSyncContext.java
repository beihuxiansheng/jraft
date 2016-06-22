package net.data.technology.jraft;

public class SnapshotSyncContext {

    private Snapshot snapshot;
    private long offset;

    public SnapshotSyncContext(Snapshot snapshot){
        this.snapshot = snapshot;
        this.offset = 0L;
    }

    public Snapshot getSnapshot(){
        return this.snapshot;
    }

    public void setOffset(long offset){
        this.offset = offset;
    }

    public long getOffset(){
        return this.offset;
    }
}
