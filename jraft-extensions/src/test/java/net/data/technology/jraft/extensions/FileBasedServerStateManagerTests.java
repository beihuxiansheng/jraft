package net.data.technology.jraft.extensions;

import static org.junit.Assert.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.data.technology.jraft.ClusterConfiguration;
import net.data.technology.jraft.ClusterServer;
import net.data.technology.jraft.ServerState;

public class FileBasedServerStateManagerTests {

	private Random random = new Random(Calendar.getInstance().getTimeInMillis());
	
	@Test
	public void testStateManager() throws IOException{
		Path container = Files.createTempDirectory("logstore");
		Files.deleteIfExists(container.resolve("store.idex"));
		Files.deleteIfExists(container.resolve("store.data"));
		Files.deleteIfExists(container.resolve("server.state"));
		Files.deleteIfExists(container.resolve("config.properties"));
		Files.deleteIfExists(container.resolve("cluster.json"));
		int serverId = this.random.nextInt();
		ClusterConfiguration config = this.randomConfiguration();
		Properties props = new Properties();
		props.put("server.id", String.valueOf(serverId));
		FileOutputStream stream = new FileOutputStream(container.resolve("config.properties").toString());
		props.store(stream, null);
		stream.flush();
		stream.close();
		Gson gson = new GsonBuilder().create();
		String data = gson.toJson(config);
		stream = new FileOutputStream(container.resolve("cluster.json").toString());
		stream.write(data.getBytes(StandardCharsets.UTF_8));
		stream.flush();
		stream.close();
		FileBasedServerStateManager manager = new FileBasedServerStateManager(container.toString());
		assertTrue(manager.loadLogStore() != null);
		assertTrue(manager.readState() == null);
		int rounds = 50 + this.random.nextInt(100);
		while(rounds > 0){
			ServerState state = new ServerState();
			state.setTerm(this.random.nextLong());
			state.setCommitIndex(this.random.nextLong());
			state.setVotedFor(this.random.nextInt());
			manager.persistState(state);
			ServerState state1 = manager.readState();
			assertTrue(state1 != null);
			assertEquals(state.getTerm(), state1.getTerm());
			assertEquals(state.getCommitIndex(), state1.getCommitIndex());
			assertEquals(state.getVotedFor(), state1.getVotedFor());
			rounds -= 1;
		}
		
		ClusterConfiguration config1 = manager.loadClusterConfiguration();
		assertConfigEquals(config, config1);
		config = this.randomConfiguration();
		manager.saveClusterConfiguration(config);
		config1 = manager.loadClusterConfiguration();
		assertConfigEquals(config, config1);
		
		// clean up
		manager.close();
		Files.deleteIfExists(container.resolve("store.idx"));
		Files.deleteIfExists(container.resolve("store.data"));
		Files.deleteIfExists(container.resolve("server.state"));
		Files.deleteIfExists(container.resolve("config.properties"));
		Files.deleteIfExists(container.resolve("cluster.json"));
		Files.deleteIfExists(container);
	}
	
	private ClusterConfiguration randomConfiguration(){
		ClusterConfiguration config = new ClusterConfiguration();
		config.setLastLogIndex(random.nextLong());
		config.setLogIndex(random.nextLong());
		int servers = random.nextInt(10) + 1;
		for(int i = 0; i < servers; ++i){
			ClusterServer server = new ClusterServer();
			server.setId(random.nextInt());
			server.setEndpoint(String.format("Server %d", (i + 1)));
			config.getServers().add(server);
		}
		
		return config;
	}
	
	private static void assertConfigEquals(ClusterConfiguration config, ClusterConfiguration config1){
		assertEquals(config.getLastLogIndex(), config1.getLastLogIndex());
		assertEquals(config.getLogIndex(), config1.getLogIndex());
		assertEquals(config.getServers().size(), config1.getServers().size());
		for(int i = 0; i < config.getServers().size(); ++i){
			ClusterServer s1 = config.getServers().get(i);
			ClusterServer s2 = config.getServers().get(i);
			assertEquals(s1.getId(), s2.getId());
			assertEquals(s1.getEndpoint(), s2.getEndpoint());
		}
	}

}
