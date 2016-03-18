package net.data.technology.jraft.extensions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;

public class BinaryUtils {

	public static byte[] longToBytes(long value){
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(value);
		return buffer.array();
	}
	
	public static long bytesToLong(byte[] bytes, int offset){
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes, offset, Long.BYTES);
		buffer.flip();
		return buffer.getLong();
	}
	
	public static byte[] intToBytes(int value){
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		buffer.putInt(value);
		return buffer.array();
	}
	
	public static long bytesToInt(byte[] bytes, int offset){
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
		buffer.put(bytes, offset, Integer.BYTES);
		buffer.flip();
		return buffer.getInt();
	}
	
	public static byte booleanToByte(boolean value){
		return value ? (byte)1 : (byte)0;
	}
	
	public static boolean byteToBoolean(byte value){
		return value != 0;
	}
	
	public static byte[] messageToBytes(RaftResponseMessage response){
		ByteBuffer buffer = ByteBuffer.allocate(26);
		buffer.put(response.getMessageType().toByte());
		buffer.put(intToBytes(response.getSource()));
		buffer.put(intToBytes(response.getDestination()));
		buffer.put(longToBytes(response.getTerm()));
		buffer.put(longToBytes(response.getNextIndex()));
		buffer.put(booleanToByte(response.isAccepted()));
		return buffer.array();
	}
	
	public static byte[] messageToBytes(RaftRequestMessage request){
		LogEntry[] logEntries = request.getLogEntries();
		int logSize = 0;
		List<byte[]> buffersForLogs = null;
		if(logEntries != null && logEntries.length > 0){
			buffersForLogs = new ArrayList<byte[]>(logEntries.length);
			for(LogEntry logEntry : logEntries){
				byte[] logData = logEntryToBytes(logEntry);
				logSize += logData.length;
				buffersForLogs.add(logData);
			}
		}
		
		ByteBuffer requestBuffer = ByteBuffer.allocate(45 + logSize);
		requestBuffer.put(request.getMessageType().toByte());
		requestBuffer.put(intToBytes(request.getSource()));
		requestBuffer.put(intToBytes(request.getDestination()));
		requestBuffer.put(longToBytes(request.getTerm()));
		requestBuffer.put(longToBytes(request.getLastLogTerm()));
		requestBuffer.put(longToBytes(request.getLastLogIndex()));
		requestBuffer.put(longToBytes(request.getCommitIndex()));
		requestBuffer.put(intToBytes(logSize));
		if(buffersForLogs != null){
			for(byte[] logData : buffersForLogs){
				requestBuffer.put(logData);
			}
		}
		
		return requestBuffer.array();
	}
	
	public static byte[] logEntryToBytes(LogEntry logEntry){
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try{
			output.write(longToBytes(logEntry.getTerm()));
			output.write(intToBytes(logEntry.getValue().length));
			output.write(logEntry.getValue());
			output.flush();
			return output.toByteArray();
		}catch(IOException exception){
			LogManager.getLogger("BinaryUtil").error("failed to serialize LogEntry to memory", exception);
			throw new RuntimeException("Running into bad situation, where memory may not be sufficient", exception);
		}
	}
}
