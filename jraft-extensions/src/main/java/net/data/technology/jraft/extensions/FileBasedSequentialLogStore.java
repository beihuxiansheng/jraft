package net.data.technology.jraft.extensions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
	private static final LogEntry zeroEntry = new LogEntry();
	
	private Logger logger;
	private RandomAccessFile indexFile;
	private RandomAccessFile dataFile;
	private long entriesInStore;
	private LogEntry lastEntry;
	
	public FileBasedSequentialLogStore(final String logContainer){
		String logFilePrefix = logContainer.endsWith(File.separator) ? logContainer : logContainer + File.separator;
		this.logger = LogManager.getLogger(getClass());
		try{
			this.indexFile = new RandomAccessFile(logFilePrefix + LOG_INDEX_FILE, "rw");
			this.dataFile = new RandomAccessFile(logFilePrefix + LOG_STORE_FILE, "rw");
			this.entriesInStore = this.indexFile.length() / Long.BYTES;
			this.loadLastEntry();
		}catch(IOException exception){
			this.logger.error("failed to access log store", exception);
		}
	}
	
	@Override
	public synchronized long getFirstAvailableIndex() {
		return this.entriesInStore + 1;
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
		this.writeAt(this.entriesInStore + 1, logEntry);
	}

	/**
	 * write the log entry at the specific index, all log entries after index will be discarded
	 * @param index starts from 1
	 * @param logEntry
	 */
	@Override
	public synchronized void writeAt(long index, LogEntry logEntry) {
		try{
			ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + logEntry.getValue().length);
			buffer.put(BinaryUtils.longToBytes(logEntry.getTerm()));
			buffer.put(logEntry.getValueType().toByte());
			buffer.put(logEntry.getValue());
			
			// find the positions for index and data files
			long dataPosition = this.dataFile.length();
			long indexPosition = (index - 1) * Long.BYTES;
			if(indexPosition < this.indexFile.length()){
				this.indexFile.seek(indexPosition);
				byte[] longBuffer = new byte[Long.BYTES];
				this.read(this.indexFile, longBuffer);
				dataPosition = BinaryUtils.bytesToLong(longBuffer, 0);
			}
			
			// write the data at the specified position
			this.indexFile.seek(indexPosition);
			this.dataFile.seek(dataPosition);
			this.indexFile.write(BinaryUtils.longToBytes(dataPosition));
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
	public synchronized LogEntry[] getLogEntries(long start, long end) {
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
			byte[] dataPositionBuffer = new byte[(entriesToRead + 1) * Long.BYTES];
			this.indexFile.seek((start - 1) * Long.BYTES);
			this.read(this.indexFile, dataPositionBuffer);
			for(int i = 0; i < entriesToRead; ++i){
				long dataStart = BinaryUtils.bytesToLong(dataPositionBuffer, i * Long.BYTES);
				long dataEnd = BinaryUtils.bytesToLong(dataPositionBuffer, (i + 1) * Long.BYTES);
				int dataSize = (int)(dataEnd - dataStart);
				byte[] logData = new byte[dataSize];
				this.dataFile.seek(dataStart);
				this.read(this.dataFile, logData);
				entries[i] = new LogEntry(BinaryUtils.bytesToLong(logData, 0), Arrays.copyOfRange(logData, Long.BYTES + 1, logData.length), LogValueType.fromByte(logData[Long.BYTES]));
			}
			
			return entries;
		}catch(IOException exception){
			this.logger.error("failed to read entries from store", exception);
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	@Override
	public synchronized LogEntry getLogEntryAt(long index) {
		if(index > this.entriesInStore){
			return null;
		}
		
		if(index == this.entriesInStore){
			return this.lastEntry;
		}
		
		try{
			long indexPosition = (index - 1) * Long.BYTES;
			this.indexFile.seek(indexPosition);
			byte[] longBuffer = new byte[Long.BYTES * 2];
			this.read(this.indexFile, longBuffer);
			long dataPosition = BinaryUtils.bytesToLong(longBuffer, 0);
			long endDataPosition = BinaryUtils.bytesToLong(longBuffer, Long.BYTES);
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
	public synchronized byte[] packLog(long index, int itemsToPack){
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
				byte[] endOfLogIndexData = new byte[Long.BYTES];
				this.read(this.indexFile, endOfLogIndexData);
				endOfLog = BinaryUtils.bytesToLong(endOfLogIndexData, 0);
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
	public synchronized void applyLogPack(long index, byte[] logPack){
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
				byte[] longValueBuffer = new byte[Long.BYTES];
				this.indexFile.seek(indexFilePosition);
				this.read(this.indexFile, longValueBuffer);
				dataFilePosition = BinaryUtils.bytesToLong(longValueBuffer, 0);
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
	
	public void close(){
		try{
			this.dataFile.close();
			this.indexFile.close();
		}catch(IOException exception){
			this.logger.error("failed to close data/index file(s)", exception);
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
