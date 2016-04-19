package net.data.technology.jraft.extensions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.SequentialLogStore;

public class FileBasedSequentialLogStore implements SequentialLogStore {

	private static final String LOG_INDEX_FILE = "store.idx";
	private static final String LOG_STORE_FILE = "store.data";
	private static final String LOG_START_INDEX_FILE = "store.sti";
	private static final String LOG_INDEX_FILE_BAK = "store.idx.bak";
	private static final String LOG_STORE_FILE_BAK = "store.data.bak";
	private static final String LOG_START_INDEX_FILE_BAK = "store.sti.bak";
	private static final LogEntry zeroEntry = new LogEntry();
	
	private Logger logger;
	private RandomAccessFile indexFile;
	private RandomAccessFile dataFile;
	private RandomAccessFile startIndexFile;
	private long entriesInStore;
	private long startIndex;
	private LogEntry lastEntry;
	private Path logContainer;
	
	public FileBasedSequentialLogStore(String logContainer){
		this.logContainer = Paths.get(logContainer);
		this.logger = LogManager.getLogger(getClass());
		try{
			this.indexFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE).toString(), "rw");
			this.dataFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE).toString(), "rw");
			this.startIndexFile = new RandomAccessFile(this.logContainer.resolve(LOG_START_INDEX_FILE).toString(), "rw");
			if(this.startIndexFile.length() == 0){
				this.startIndex = 1;
				this.startIndexFile.writeLong(this.startIndex);
			}else{
				this.startIndex = this.startIndexFile.readLong();
			}
			
			this.entriesInStore = this.indexFile.length() / Long.BYTES;
			this.loadLastEntry();
			this.logger.debug(String.format("log store started with entriesInStore=%d, startIndex=%d", this.entriesInStore, this.startIndex));
		}catch(IOException exception){
			this.logger.error("failed to access log store", exception);
		}
	}
	
	@Override
	public synchronized long getFirstAvailableIndex() {
		return this.entriesInStore + this.startIndex;
	}
	
	@Override
	public synchronized long getStartIndex() {
		return this.startIndex;
	}

	@Override
	public synchronized LogEntry getLastLogEntry() {
		if(this.lastEntry == null){
			return zeroEntry;
		}
		
		return this.lastEntry;
	}

	@Override
	public synchronized void append(LogEntry logEntry) {
		this.writeAt(this.entriesInStore + this.startIndex, logEntry);
	}

	/**
	 * write the log entry at the specific index, all log entries after index will be discarded
	 * @param logIndex must be >= this.getStartIndex()
	 * @param logEntry
	 */
	@Override
	public synchronized void writeAt(long logIndex, LogEntry logEntry) {
		if(logIndex < this.startIndex){
			throw new IllegalArgumentException("logIndex out of range");
		}
		
		long index = logIndex - this.startIndex + 1; //start index is one based
		try{
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + logEntry.getValue().length);
			buffer.putLong(logEntry.getTerm());
			buffer.put(logEntry.getValueType().toByte());
			buffer.put(logEntry.getValue());
			
			// find the positions for index and data files
			long dataPosition = this.dataFile.length();
			long indexPosition = (index - 1) * Long.BYTES;
			if(indexPosition < this.indexFile.length()){
				this.indexFile.seek(indexPosition);
				dataPosition = this.indexFile.readLong();
			}
			
			// write the data at the specified position
			this.indexFile.seek(indexPosition);
			this.dataFile.seek(dataPosition);
			this.indexFile.writeLong(dataPosition);
			this.dataFile.write(buffer.array());
			
			// trim the files if necessary
			if(this.indexFile.length() > this.indexFile.getFilePointer()){
				this.indexFile.setLength(this.indexFile.getFilePointer());
			}
			
			if(this.dataFile.length() > this.dataFile.getFilePointer()){
				this.dataFile.setLength(this.dataFile.getFilePointer());
			}
			
			this.entriesInStore = index;
			this.lastEntry = logEntry;
		}catch(IOException exception){
			this.logger.error("failed to append a log entry to store", exception);
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	@Override
	public synchronized LogEntry[] getLogEntries(long startIndex, long endIndex) {
		if(startIndex < this.startIndex){
			throw new IllegalArgumentException("startIndex out of range");
		}
		
		long start = startIndex - this.startIndex + 1;
		long end = endIndex - this.startIndex + 1;
		long adjustedEnd = end > this.entriesInStore + 1 ? this.entriesInStore + 1 : end;
		if(adjustedEnd == this.entriesInStore + 1 && start == this.entriesInStore){
			return new LogEntry[] { this.lastEntry };
		}
		try{
			LogEntry[] entries = new LogEntry[(int)(adjustedEnd - start)];
			if(entries.length == 0){
				return entries;
			}
			
			if(adjustedEnd == this.entriesInStore + 1){
				entries[entries.length - 1] = this.lastEntry;
				adjustedEnd -= 1;
			}
			
			int entriesToRead = (int)(adjustedEnd - start);
			this.indexFile.seek((start - 1) * Long.BYTES);
			long dataStart = this.indexFile.readLong();
			for(int i = 0; i < entriesToRead; ++i){
				long dataEnd = this.indexFile.readLong();
				int dataSize = (int)(dataEnd - dataStart);
				byte[] logData = new byte[dataSize];
				this.dataFile.seek(dataStart);
				this.read(this.dataFile, logData);
				entries[i] = new LogEntry(BinaryUtils.bytesToLong(logData, 0), Arrays.copyOfRange(logData, Long.BYTES + 1, logData.length), LogValueType.fromByte(logData[Long.BYTES]));
				dataStart = dataEnd;
			}
			
			return entries;
		}catch(IOException exception){
			this.logger.error("failed to read entries from store", exception);
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	@Override
	public synchronized LogEntry getLogEntryAt(long logIndex) {
		if(logIndex < this.startIndex){
			this.logger.error(String.format("startIndex is %d, while index at %d is requested", this.startIndex, logIndex));
			throw new IllegalArgumentException("logIndex out of range");
		}
		
		long index = logIndex - this.startIndex + 1;
		if(index > this.entriesInStore){
			return null;
		}
		
		if(index == this.entriesInStore){
			return this.lastEntry;
		}
		
		try{
			long indexPosition = (index - 1) * Long.BYTES;
			this.indexFile.seek(indexPosition);
			long dataPosition = this.indexFile.readLong();
			long endDataPosition = this.indexFile.readLong();
			this.dataFile.seek(dataPosition);
			byte[] logData = new byte[(int)(endDataPosition - dataPosition)];
			this.read(this.dataFile, logData);
			return new LogEntry(BinaryUtils.bytesToLong(logData, 0), Arrays.copyOfRange(logData, Long.BYTES + 1, logData.length), LogValueType.fromByte(logData[Long.BYTES]));
		}catch(IOException exception){
			this.logger.error("failed to read files to get the specified entry");
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}
	
	@Override
	public synchronized byte[] packLog(long logIndex, int itemsToPack){
		if(logIndex < this.startIndex){
			throw new IllegalArgumentException("logIndex out of range");
		}
		
		long index = logIndex - this.startIndex + 1;
		if(index > this.entriesInStore){
			return new byte[0];
		}
		
		long endIndex = Math.min(index + itemsToPack, this.entriesInStore + 1);
		boolean readToEnd = (endIndex == this.entriesInStore + 1);
		try{
			long indexPosition = (index - 1) * Long.BYTES;
			this.indexFile.seek(indexPosition);
			byte[] indexBuffer = new byte[(int)(Long.BYTES * (endIndex - index))];
			this.read(this.indexFile, indexBuffer);
			long endOfLog = this.dataFile.length();
			if(!readToEnd){
				endOfLog = this.indexFile.readLong();
			}
			
			long startOfLog = BinaryUtils.bytesToLong(indexBuffer, 0);
			byte[] logBuffer = new byte[(int)(endOfLog - startOfLog)];
			this.dataFile.seek(startOfLog);
			this.read(this.dataFile, logBuffer);
			ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
			GZIPOutputStream gzipStream = new GZIPOutputStream(memoryStream);
			gzipStream.write(BinaryUtils.intToBytes(indexBuffer.length));
			gzipStream.write(BinaryUtils.intToBytes(logBuffer.length));
			gzipStream.write(indexBuffer);
			gzipStream.write(logBuffer);
			gzipStream.flush();
			memoryStream.flush();
			gzipStream.close();
			return memoryStream.toByteArray();
		}catch(IOException exception){
			this.logger.error("failed to read files to read data for packing");
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}
	
	@Override
	public synchronized void applyLogPack(long logIndex, byte[] logPack){
		if(logIndex < this.startIndex){
			throw new IllegalArgumentException("logIndex out of range");
		}
		
		long index = logIndex - this.startIndex + 1;
		try{
			ByteArrayInputStream memoryStream = new ByteArrayInputStream(logPack);
			GZIPInputStream gzipStream = new GZIPInputStream(memoryStream);
			byte[] sizeBuffer = new byte[Integer.BYTES];
			this.read(gzipStream, sizeBuffer);
			int indexDataSize = BinaryUtils.bytesToInt(sizeBuffer, 0);
			this.read(gzipStream, sizeBuffer);
			int logDataSize = BinaryUtils.bytesToInt(sizeBuffer, 0);
			byte[] indexBuffer = new byte[indexDataSize];
			this.read(gzipStream, indexBuffer);
			byte[] logBuffer = new byte[logDataSize];
			this.read(gzipStream, logBuffer);
			long indexFilePosition, dataFilePosition;
			if(index == this.entriesInStore + 1){
				indexFilePosition = this.indexFile.length();
				dataFilePosition = this.dataFile.length();
			}else{
				indexFilePosition = (index - 1) * Long.BYTES;
				this.indexFile.seek(indexFilePosition);
				dataFilePosition = this.indexFile.readLong();
			}
			
			this.indexFile.seek(indexFilePosition);
			this.indexFile.write(indexBuffer);
			this.indexFile.setLength(this.indexFile.getFilePointer());
			this.dataFile.seek(dataFilePosition);
			this.dataFile.write(logBuffer);
			this.dataFile.setLength(this.dataFile.getFilePointer());
			this.entriesInStore = index - 1 + indexBuffer.length / Long.BYTES;
			gzipStream.close();
			this.loadLastEntry();
		}catch(IOException exception){
			this.logger.error("failed to write files to unpack logs for data");
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}
	
	@Override
	public synchronized boolean compact(long lastLogIndex){
		if(lastLogIndex < this.startIndex){
			throw new IllegalArgumentException("lastLogIndex out of range");
		}
		
		if(lastLogIndex == this.startIndex){
			this.logger.info("no compacting is needed");
			return true;
		}

		this.backup();
		long lastIndex = lastLogIndex - this.startIndex;
		if(lastLogIndex >= this.getFirstAvailableIndex() - 1){
			try{
				this.indexFile.setLength(0);
				this.dataFile.setLength(0);
				this.startIndexFile.seek(0);
				this.startIndexFile.writeLong(lastLogIndex + 1);
				this.startIndex = lastLogIndex + 1;
				this.entriesInStore = 0;
				this.lastEntry = null;
				return true;
			}catch(Exception e){
				this.logger.error("failed to remove data or save the start index", e);
				this.restore();
				return false;
			}
		}else{
			long dataPosition = -1;
			long indexPosition = Long.BYTES * (lastIndex + 1);
			byte[] dataPositionBuffer = new byte[Long.BYTES];
			
			try{
				this.indexFile.seek(indexPosition);
				this.read(this.indexFile, dataPositionBuffer);
				dataPosition = ByteBuffer.wrap(dataPositionBuffer).getLong();
				long indexFileNewLength = this.indexFile.length() - indexPosition;
				long dataFileNewLength = this.dataFile.length() - dataPosition;
				
				// copy the log data
				RandomAccessFile backupFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE_BAK).toString(), "r");
				FileChannel backupChannel = backupFile.getChannel();
				backupChannel.position(dataPosition);
				FileChannel channel = this.dataFile.getChannel();
				channel.transferFrom(backupChannel, 0, dataFileNewLength);
				this.dataFile.setLength(dataFileNewLength);
				backupFile.close();
				
				// copy the index data
				backupFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE_BAK).toString(), "r");
				backupFile.seek(indexPosition);
				this.indexFile.seek(0);
				for(int i = 0; i < indexFileNewLength / Long.BYTES; ++i){
					this.indexFile.writeLong(backupFile.readLong() - dataPosition);
				}
				
				this.indexFile.setLength(indexFileNewLength);
				backupFile.close();
				
				// save the starting index
				this.startIndexFile.seek(0);
				this.startIndexFile.write(ByteBuffer.allocate(Long.BYTES).putLong(lastLogIndex + 1).array());
				this.entriesInStore -= (lastLogIndex - this.startIndex + 1);
				this.startIndex = lastLogIndex + 1;
				return true;
			}catch(Throwable error){
				this.logger.error("fail to compact the logs due to error", error);
				this.restore();
				return false;
			}
		}
	}
	
	public void close(){
		try{
			this.dataFile.close();
			this.indexFile.close();
			this.startIndexFile.close();
		}catch(IOException exception){
			this.logger.error("failed to close data/index file(s)", exception);
		}
	}
	
	private void restore(){
		try{
			this.indexFile.close();
			this.dataFile.close();
			this.startIndexFile.close();
			Files.copy(this.logContainer.resolve(LOG_INDEX_FILE_BAK), this.logContainer.resolve(LOG_INDEX_FILE), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(this.logContainer.resolve(LOG_STORE_FILE_BAK), this.logContainer.resolve(LOG_STORE_FILE), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(this.logContainer.resolve(LOG_START_INDEX_FILE_BAK), this.logContainer.resolve(LOG_START_INDEX_FILE), StandardCopyOption.REPLACE_EXISTING);
			this.indexFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE).toString(), "rw");
			this.dataFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE).toString(), "rw");
			this.startIndexFile = new RandomAccessFile(this.logContainer.resolve(LOG_START_INDEX_FILE).toString(), "rw");
		}catch(Exception error){
			// this is fatal...
			this.logger.fatal("cannot restore from failure, please manually restore the log files");
			System.exit(-1);
		}
	}
	
	private void backup(){
		try {
			Files.deleteIfExists(this.logContainer.resolve(LOG_INDEX_FILE_BAK));
			Files.deleteIfExists(this.logContainer.resolve(LOG_STORE_FILE_BAK));
			Files.deleteIfExists(this.logContainer.resolve(LOG_START_INDEX_FILE_BAK));
			Files.copy(this.logContainer.resolve(LOG_INDEX_FILE), this.logContainer.resolve(LOG_INDEX_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(this.logContainer.resolve(LOG_STORE_FILE), this.logContainer.resolve(LOG_STORE_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(this.logContainer.resolve(LOG_START_INDEX_FILE), this.logContainer.resolve(LOG_START_INDEX_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			this.logger.error("failed to create a backup folder", e);
			throw new RuntimeException("failed to create a backup folder");
		}
	}
	
	private void read(InputStream stream, byte[] buffer){
		try{
			int offset = 0;
			int bytesRead = 0;
			while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
				offset += bytesRead;
			}
			
			if(offset < buffer.length){
				this.logger.error(String.format("only %d bytes are read while %d bytes are desired, bad file", offset, buffer.length));
				throw new RuntimeException("bad file, insufficient file data for reading");
			}
		}catch(IOException exception){
			this.logger.error("failed to read and fill the buffer", exception);
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}
	
	private void read(RandomAccessFile stream, byte[] buffer){
		try{
			int offset = 0;
			int bytesRead = 0;
			while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
				offset += bytesRead;
			}
			
			if(offset < buffer.length){
				this.logger.error(String.format("only %d bytes are read while %d bytes are desired, bad file", offset, buffer.length));
				throw new RuntimeException("bad file, insufficient file data for reading");
			}
		}catch(IOException exception){
			this.logger.error("failed to read and fill the buffer", exception);
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}
	
	private void loadLastEntry() throws IOException{
		long lastEntryIndex = 0;
		if(this.indexFile.length() > 0){
			this.indexFile.seek(this.indexFile.length() - Long.BYTES);
			byte[] indexBuffer = new byte[Long.BYTES];
			this.read(this.indexFile, indexBuffer);
			lastEntryIndex = BinaryUtils.bytesToLong(indexBuffer, 0);
		}
		
		if(this.dataFile.length() > 0){
			this.dataFile.seek(lastEntryIndex);
			int lastEntrySize = (int)(this.dataFile.length() - lastEntryIndex);
			if(lastEntrySize > 0){
				byte[] lastEntryBuffer = new byte[lastEntrySize];
				this.read(this.dataFile, lastEntryBuffer);
				long term = BinaryUtils.bytesToLong(lastEntryBuffer, 0);
				byte valueType = lastEntryBuffer[Long.BYTES];
				this.lastEntry = new LogEntry(term, Arrays.copyOfRange(lastEntryBuffer, Long.BYTES + 1, lastEntrySize), LogValueType.fromByte(valueType));
			}
		}
	}
}
