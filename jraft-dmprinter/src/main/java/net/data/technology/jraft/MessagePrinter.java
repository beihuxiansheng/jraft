package net.data.technology.jraft;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.log4j.LogManager;

public class MessagePrinter implements StateMachine {

	private Path snapshotStore;
	private long commitIndex;
	private List<String> messages = new LinkedList<String>();
	private boolean snapshotInprogress = false;
	
	public MessagePrinter(Path baseDir){
		this.snapshotStore = baseDir.resolve("snapshots");
		this.commitIndex = 0;
		if(!Files.isDirectory(this.snapshotStore)){
			try{
				Files.createDirectory(this.snapshotStore);
			}catch(Exception error){
				throw new IllegalArgumentException("bad baseDir");
			}
		}
	}
	
	@Override
	public void commit(long logIndex, byte[] data) {
		String message = new String(data, StandardCharsets.UTF_8);
		System.out.printf("commit: %d\t%s\n", logIndex, message);
		synchronized(this.messages){
			this.commitIndex = logIndex;
			this.messages.add(message);
		}
	}

	@Override
	public void saveSnapshotData(Snapshot snapshot, long offset, byte[] data) {
		Path filePath = this.snapshotStore.resolve(String.format("%d-%d.s", snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));
		try{
			if(!Files.exists(filePath)){
				Files.write(this.snapshotStore.resolve(String.format("%d.cnf", snapshot.getLastLogIndex())), snapshot.getLastConfig().toBytes(), StandardOpenOption.CREATE);
			}
			
			RandomAccessFile snapshotFile = new RandomAccessFile(filePath.toString(), "rw");
			snapshotFile.seek(offset);
			snapshotFile.write(data);
			snapshotFile.close();
		}catch(Exception error){
			throw new RuntimeException(error.getMessage());
		}
	}

	@Override
	public boolean applySnapshot(Snapshot snapshot) {
		Path filePath = this.snapshotStore.resolve(String.format("%d-%d.s", snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));
		if(!Files.exists(filePath)){
			return false;
		}
		
		try{
			FileInputStream input = new FileInputStream(filePath.toString());
			InputStreamReader reader = new InputStreamReader(input, StandardCharsets.UTF_8);
			BufferedReader bufferReader = new BufferedReader(reader);
			synchronized(this.messages){
				this.messages.clear();
				String line = null;
				while((line = bufferReader.readLine()) != null){
					if(line.length() > 0){
						System.out.printf("from snapshot: %s\n", line);
						this.messages.add(line);
					}
				}
				
				this.commitIndex = snapshot.getLastLogIndex();
			}
			
			bufferReader.close();
			reader.close();
			input.close();
		}catch(Exception error){
			LogManager.getLogger(getClass()).error("failed to apply the snapshot", error);
			return false;
		}
		return true;
	}

	@Override
	public int readSnapshotData(Snapshot snapshot, long offset, byte[] buffer) {
		Path filePath = this.snapshotStore.resolve(String.format("%d-%d.s", snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));
		if(!Files.exists(filePath)){
			return -1;
		}
		
		try{
			RandomAccessFile snapshotFile = new RandomAccessFile(filePath.toString(), "rw");
			snapshotFile.seek(offset);
			int bytesRead = read(snapshotFile, buffer);
			snapshotFile.close();
			return bytesRead;
		}catch(Exception error){
			LogManager.getLogger(getClass()).error("failed read data from snapshot", error);
			return -1;
		}
	}

	@Override
	public CompletableFuture<Boolean> createSnapshot(Snapshot snapshot) {
		if(snapshot.getLastLogIndex() > this.commitIndex){
			return CompletableFuture.completedFuture(false);
		}
		
		List<String> copyOfMessages = new LinkedList<String>();
		synchronized(this.messages){
			if(this.snapshotInprogress){
				return CompletableFuture.completedFuture(false);
			}
			
			this.snapshotInprogress = true;
			copyOfMessages.addAll(this.messages);
		}
		
		return CompletableFuture.supplyAsync(() -> {
			Path filePath = this.snapshotStore.resolve(String.format("%d-%d.s", snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));
			try{
				if(!Files.exists(filePath)){
					Files.write(this.snapshotStore.resolve(String.format("%d.cnf", snapshot.getLastLogIndex())), snapshot.getLastConfig().toBytes(), StandardOpenOption.CREATE);
				}
				
				FileOutputStream stream = new FileOutputStream(filePath.toString());
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
				for(String msg: copyOfMessages){
					writer.write(msg);
					writer.write('\n');
				}
				writer.flush();
				writer.close();
				stream.close();
				synchronized(this.messages){
					this.snapshotInprogress = false;
				}
				return true;
			}catch(Exception error){
				throw new RuntimeException(error.getMessage());
			}
		});
	}

	@Override
	public Snapshot getLastSnapshot() {
		try{
			Stream<Path> files = Files.list(this.snapshotStore);
			Path latestSnapshot = null;
			long maxLastLogIndex = 0;
			long term = 0;
			Pattern pattern = Pattern.compile("(\\d+)\\-(\\d+)\\.s");
			Iterator<Path> itor = files.iterator();
			while(itor.hasNext()){
				Path file = itor.next();
				if(Files.isRegularFile(file)){
					Matcher matcher = pattern.matcher(file.getFileName().toString());
					if(matcher.matches()){
						long lastLogIndex = Long.parseLong(matcher.group(1));
						if(lastLogIndex > maxLastLogIndex){
							maxLastLogIndex = lastLogIndex;
							term = Long.parseLong(matcher.group(2));
							latestSnapshot = file;
						}
					}
				}
			}
			
			files.close();
			if(latestSnapshot != null){
				byte[] configData = Files.readAllBytes(this.snapshotStore.resolve(String.format("%d.cnf", maxLastLogIndex)));
				ClusterConfiguration config = ClusterConfiguration.fromBytes(configData);
				return new Snapshot(maxLastLogIndex, term, config, latestSnapshot.toFile().length());
			}
		}catch(Exception error){
			LogManager.getLogger(getClass()).error("failed read snapshot info snapshot store", error);
		}
		
		return null;
	}
	
	private static int read(RandomAccessFile stream, byte[] buffer){
		try{
			int offset = 0;
			int bytesRead = 0;
			while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
				offset += bytesRead;
			}
			
			return offset;
		}catch(IOException exception){
			return -1;
		}
	}

	@Override
	public void rollback(long logIndex, byte[] data) {
		System.out.println(String.format("Rollback index %d", logIndex));
	}

	@Override
	public void preCommit(long logIndex, byte[] data) {
		System.out.println(String.format("PreCommit:%s at %d", new String(data, StandardCharsets.UTF_8), logIndex));
	}
}
