package net.data.technology.jraft;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
    	
    	Path configPath = baseDir.resolve("jraft.json");
    	if(!Files.isRegularFile(configPath)){
    		System.out.println("jraft.json does not exist, please create one to hold the configuration");
    		return;
    	}
    	
    	// load configuration
    	Gson gson = new GsonBuilder().create();
    	ClusterConfiguration config = gson.fromJson(new InputStreamReader(new FileInputStream(configPath.toFile()), "utf-8"), ClusterConfiguration.class);
    	
    	if("client".equalsIgnoreCase(args[0])){
    		executeAsClient(config);
    		return;
    	}
    	
    	URI localEndpoint = new URI(config.getLocalServer().getEndpoint());
    	FileBasedServerStateManager stateManager = new FileBasedServerStateManager(args[1]);
    	RaftParameters raftParameters = new RaftParameters(5000, 3000, 1500, 500);
    	RaftContext context = new RaftContext(
    			stateManager,
    			new MessagePrinter(),
    			raftParameters,
    			new RpcTcpListener(localEndpoint.getPort()),
    			new Log4jLoggerFactory(),
    			new RpcTcpClientFactory());
    	RaftConsensus.run(context, config);
        System.out.println( "Press any key to exit." );
        System.in.read();
    }
    
    private static void executeAsClient(ClusterConfiguration configuration) throws Exception{
    	RaftClient client = new RaftClient(new RpcTcpClientFactory(), configuration, new Log4jLoggerFactory());
    	BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    	while(true){
    		System.out.print("Message:");
    		String message = reader.readLine();
    		boolean accepted = client.appendEntries(new byte[][]{ message.getBytes() }).get();
    		System.out.println("Accepted: " + String.valueOf(accepted));
    	}
    }
}
