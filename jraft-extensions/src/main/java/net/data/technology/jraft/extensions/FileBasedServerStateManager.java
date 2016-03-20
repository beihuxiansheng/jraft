package net.data.technology.jraft.extensions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.SequentialLogStore;
import net.data.technology.jraft.ServerState;
import net.data.technology.jraft.ServerStateManager;

public class FileBasedServerStateManager implements ServerStateManager {

	private static final String STATE_FILE = "server.state";
	private RandomAccessFile serverStateFile;
	private FileBasedSequentialLogStore logStore;
	private Logger logger;
	
	public FileBasedServerStateManager(String dataDirectory){
		this.logStore = new FileBasedSequentialLogStore(dataDirectory);
		this.logger = LogManager.getLogger(getClass());
		try{
			this.serverStateFile = new RandomAccessFile((dataDirectory.endsWith(File.separator) ? dataDirectory : dataDirectory + File.separator) + STATE_FILE, "rw");
			this.serverStateFile.seek(0);
		}catch(IOException exception){
			this.logger.error("failed to create/open server state file", exception);
			throw new IllegalArgumentException("cannot create/open the state file", exception);
		}
	}
	
	@Override
	public synchronized void persistState(int serverId, ServerState serverState) {
		try{
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
			buffer.putLong(serverState.getTerm());
			buffer.putInt(serverState.getVotedFor());
			this.serverStateFile.write(buffer.array());
			this.serverStateFile.seek(0);
		}catch(IOException ioError){
			this.logger.error("failed to write to the server state file", ioError);
			throw new RuntimeException("fatal I/O error while writing to the state file", ioError);
		}
	}

	@Override
	public synchronized ServerState readState(int serverId) {
		try{
			if(this.serverStateFile.length() == 0){
				return null;
			}
			
			byte[] stateData = new byte[Long.BYTES + Integer.BYTES];
			this.serverStateFile.read(stateData);
			this.serverStateFile.seek(0);
			ByteBuffer buffer = ByteBuffer.wrap(stateData);
			ServerState state = new ServerState();
			state.setTerm(buffer.getLong());
			state.setVotedFor(buffer.getInt());
			return state;
		}catch(IOException ioError){
			this.logger.error("failed to read from the server state file", ioError);
			throw new RuntimeException("fatal I/O error while reading from state file", ioError);
		}
	}

	@Override
	public SequentialLogStore loadLogStore(int serverId) {
		return this.logStore;
	}

	public void close(){
		try{
			this.serverStateFile.close();
			this.logStore.close();
		}catch(IOException exception){
			this.logger.info("failed to shutdown the server state manager due to io error", exception);
		}
	}
}
