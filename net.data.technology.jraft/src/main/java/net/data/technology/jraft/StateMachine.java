package net.data.technology.jraft;

import java.util.concurrent.CompletableFuture;

public interface StateMachine {

	public void commit(long logIndex, byte[] data);
	
	public void rollback(long logIndex, byte[] data);
	
	public void preCommit(long logIndex, byte[] data);
	
	public void saveSnapshotData(Snapshot snapshot, long offset, byte[] data);
	
	public boolean applySnapshot(Snapshot snapshot);
	
	public int readSnapshotData(Snapshot snapshot, long offset, byte[] buffer);
	
	public Snapshot getLastSnapshot();
	
	public CompletableFuture<Boolean> createSnapshot(Snapshot snapshot);
}
