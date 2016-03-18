package net.data.technology.jraft;

public interface StateMachine {

	public void commit(long logIndex, byte[] data);
}
