package net.data.technology.jraft;

import java.nio.ByteBuffer;

public class SnapshotSyncRequest {

    private Snapshot snapshot;
    private long offset;
    private byte[] data;
    private boolean done;

    public SnapshotSyncRequest(Snapshot snapshot, long offset, byte[] data, boolean done) {
        this.snapshot = snapshot;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }

    public byte[] toBytes(){
        byte[] configData = this.snapshot.getLastConfig().toBytes();
        int size = Long.BYTES * 3 + configData.length + Integer.BYTES * 2 + data.length + 1;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(snapshot.getLastLogIndex());
        buffer.putLong(snapshot.getLastLogTerm());
        buffer.putInt(configData.length);
        buffer.put(configData);
        buffer.putLong(this.offset);
        buffer.putInt(this.data.length);
        buffer.put(this.data);
        buffer.put(this.done ? (byte)1 : (byte)0);
        return buffer.array();
    }

    public static SnapshotSyncRequest fromBytes(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long lastLogIndex = buffer.getLong();
        long lastLogTerm = buffer.getLong();
        int configSize = buffer.getInt();
        ClusterConfiguration config = ClusterConfiguration.fromBytes(ByteBuffer.wrap(bytes, buffer.position(), configSize));
        buffer.position(buffer.position() + configSize);
        long offset = buffer.getLong();
        int dataSize = buffer.getInt();
        byte[] data = new byte[dataSize];
        buffer.get(data);
        boolean done = buffer.get() == 1;
        return new SnapshotSyncRequest(new Snapshot(lastLogIndex, lastLogTerm, config), offset, data, done);
    }
}
