package net.data.technology.jraft;

public interface RaftMessageHandler {

    public RaftResponseMessage processRequest(RaftRequestMessage request);
}
