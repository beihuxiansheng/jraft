package net.data.technology.jraft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.StringTokenizer;

import net.data.technology.jraft.extensions.FileBasedServerStateManager;
import net.data.technology.jraft.extensions.Log4jLoggerFactory;
import net.data.technology.jraft.extensions.RpcTcpClientFactory;
import net.data.technology.jraft.extensions.RpcTcpListener;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	if(args.length != 2){
    		System.out.println("Please specify execution mode and a base directory for this instance.");
    		return;
    	}
    	
    	if(!"server".equalsIgnoreCase(args[0]) && !"client".equalsIgnoreCase(args[0])){
    		System.out.println("only client and server modes are supported");
    		return;
    	}
    	
    	Path baseDir = Paths.get(args[1]);
    	if(!Files.isDirectory(baseDir)){
    		System.out.printf("%s does not exist as a directory\n", args[1]);
    		return;
    	}
    	
    	FileBasedServerStateManager stateManager = new FileBasedServerStateManager(args[1]);
    	ClusterConfiguration config = stateManager.loadClusterConfiguration();
    	
    	if("client".equalsIgnoreCase(args[0])){
    		executeAsClient(config);
    		return;
    	}

    	URI localEndpoint = new URI(config.getServer(stateManager.getServerId()).getEndpoint());
    	RaftParameters raftParameters = new RaftParameters(5000, 3000, 1500, 500, 5, 5);
    	RaftContext context = new RaftContext(
    			stateManager,
    			new MessagePrinter(),
    			raftParameters,
    			new RpcTcpListener(localEndpoint.getPort()),
    			new Log4jLoggerFactory(),
    			new RpcTcpClientFactory());
    	RaftConsensus.run(context);
        System.out.println( "Press any key to exit." );
        System.in.read();
    }
    
    private static void executeAsClient(ClusterConfiguration configuration) throws Exception{
    	RaftClient client = new RaftClient(new RpcTcpClientFactory(), configuration, new Log4jLoggerFactory());
    	BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    	while(true){
    		System.out.print("Message:");
    		String message = reader.readLine();
    		if(message.startsWith("srv")){
    			StringTokenizer tokenizer = new StringTokenizer(message, ";");
    			ArrayList<String> values = new ArrayList<String>();
    			while(tokenizer.hasMoreTokens()){
    				values.add(tokenizer.nextToken());
    			}
    			
    			if(values.size() == 3){
    				ClusterServer server = new ClusterServer();
    				server.setEndpoint(values.get(2));
    				server.setId(Integer.parseInt(values.get(1)));
    				boolean accepted = client.addServer(server).get();
    	    		System.out.println("Accepted: " + String.valueOf(accepted));
    				continue;
    			}
    		}
    		
    		boolean accepted = client.appendEntries(new byte[][]{ message.getBytes() }).get();
    		System.out.println("Accepted: " + String.valueOf(accepted));
    	}
    }
}
