package net.data.technology.jraft.tests;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Random;

import org.junit.Test;

import net.data.technology.jraft.ClusterConfiguration;
import net.data.technology.jraft.ClusterServer;

public class ClusterConfigurationTests {

    @Test
    public void testSerialization() {
        ClusterConfiguration config = new ClusterConfiguration();
        Random random = new Random(Calendar.getInstance().getTimeInMillis());
        config.setLastLogIndex(random.nextLong());
        config.setLogIndex(random.nextLong());
        int servers = random.nextInt(10) + 1;
        for(int i = 0; i < servers; ++i){
            ClusterServer server = new ClusterServer();
            server.setId(random.nextInt());
            server.setEndpoint(String.format("Server %d", (i + 1)));
            config.getServers().add(server);
        }

        byte[] data = config.toBytes();
        ClusterConfiguration config1 = ClusterConfiguration.fromBytes(data);
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
