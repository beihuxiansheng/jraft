package net.data.technology.jraft.extensions;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Random;

import org.junit.Test;

import net.data.technology.jraft.ServerState;

public class FileBasedServerStateManagerTests {

	private Random random = new Random(Calendar.getInstance().getTimeInMillis());
	
	@Test
	public void testStateManager() throws IOException{
		Path container = Files.createTempDirectory("logstore");
		Files.deleteIfExists(container.resolve("store.idex"));
		Files.deleteIfExists(container.resolve("store.data"));
		Files.deleteIfExists(container.resolve("server.state"));
		FileBasedServerStateManager manager = new FileBasedServerStateManager(container.toString());
		assertTrue(manager.loadLogStore(0) != null);
		assertTrue(manager.readState(0) == null);
		int rounds = 50 + this.random.nextInt(100);
		while(rounds > 0){
			ServerState state = new ServerState();
			state.setTerm(this.random.nextLong());
			state.setVotedFor(this.random.nextInt());
			manager.persistState(0, state);
			ServerState state1 = manager.readState(0);
			assertTrue(state1 != null);
			assertEquals(state.getTerm(), state1.getTerm());
			assertEquals(state.getVotedFor(), state1.getVotedFor());
			rounds -= 1;
		}
		
		// clean up
		manager.close();
		Files.deleteIfExists(container.resolve("store.idx"));
		Files.deleteIfExists(container.resolve("store.data"));
		Files.deleteIfExists(container.resolve("server.state"));
		Files.deleteIfExists(container);
	}

}
