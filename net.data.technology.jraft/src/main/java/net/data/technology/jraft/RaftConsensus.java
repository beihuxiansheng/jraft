package net.data.technology.jraft;

public class RaftConsensus {

	public static RaftMessageSender run(RaftContext context){
		if(context == null){
			throw new IllegalArgumentException("context cannot be null");
		}
		
		RaftServer server = new RaftServer(context);
		context.getRpcListener().startListening(server);
		return server.createMessageSender();
	}
}
