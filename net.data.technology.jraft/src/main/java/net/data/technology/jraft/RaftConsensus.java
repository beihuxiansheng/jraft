package net.data.technology.jraft;

public class RaftConsensus {

	public static void run(RaftContext context, ClusterConfiguration configuration){
		if(context == null){
			throw new IllegalArgumentException("context cannot be null");
		}
		
		if(configuration == null){
			throw new IllegalArgumentException("configuration cannot be null");
		}
		
		RaftServer server = new RaftServer(context, configuration);
		context.getRpcListener().startListening(server);
	}
}
