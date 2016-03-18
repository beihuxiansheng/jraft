package net.data.technology.jraft.extensions;

import java.nio.ByteBuffer;

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
		return null;
	}
}
