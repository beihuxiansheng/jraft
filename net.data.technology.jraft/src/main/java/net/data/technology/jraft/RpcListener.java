package net.data.technology.jraft;

public interface RpcListener {

    public void startListening(RaftMessageHandler messageHandler);

    public void stop();
}
