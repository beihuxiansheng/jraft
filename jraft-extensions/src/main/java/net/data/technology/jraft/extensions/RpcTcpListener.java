package net.data.technology.jraft.extensions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.RaftMessageHandler;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcListener;

public class RpcTcpListener implements RpcListener {
	private int port;
	
	public RpcTcpListener(int port){
		this.port = port;
	}

	@Override
	public void startListening(RaftMessageHandler messageHandler) {
		int processors = Runtime.getRuntime().availableProcessors();
		ExecutorService executorService = Executors.newFixedThreadPool(processors);
		try{
			AsynchronousChannelGroup channelGroup = AsynchronousChannelGroup.withThreadPool(executorService);
			AsynchronousServerSocketChannel listener = AsynchronousServerSocketChannel.open(channelGroup);
			listener.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			listener.bind(new InetSocketAddress(this.port));
			listener.accept(messageHandler, handlerFrom((AsynchronousSocketChannel connection, RaftMessageHandler handler) -> {
				readRequest(connection, handler);
			}));
		}catch(IOException exception){
			LogManager.getLogger(getClass()).error("failed to start the listener due to io error", exception);
		}
	}
	
	private static void readRequest(final AsynchronousSocketChannel connection, RaftMessageHandler messageHandler){
		ByteBuffer buffer = ByteBuffer.allocate(BinaryUtils.RAFT_REQUEST_HEADER_SIZE);
		connection.read(buffer, messageHandler, handlerFrom((Integer bytesRead, final RaftMessageHandler handler) -> {
			if(bytesRead.intValue() < BinaryUtils.RAFT_REQUEST_HEADER_SIZE){
				LogManager.getLogger(RpcTcpListener.class).info("failed to read the request header from client socket");
			}else{
				final Pair<RaftRequestMessage, Integer> requestInfo = BinaryUtils.bytesToRequestMessage(buffer.array());
				if(requestInfo.getSecond().intValue() > 0){
					ByteBuffer logBuffer = ByteBuffer.allocate(requestInfo.getSecond().intValue());
					connection.read(logBuffer, null, handlerFrom((Integer size, Object attachment) -> {
						if(size.intValue() < requestInfo.getSecond().intValue()){
							LogManager.getLogger(RpcTcpListener.class).info("failed to read the log entries data from client socket");
						}else{
							requestInfo.getFirst().setLogEntries(BinaryUtils.bytesToLogEntries(logBuffer.array()));
							processRequest(connection, requestInfo.getFirst(), handler);
						}
					}));
				}else{
					processRequest(connection, requestInfo.getFirst(), handler);
				}
			}
		}));
	}
	
	private static void processRequest(AsynchronousSocketChannel connection, RaftRequestMessage request, RaftMessageHandler messageHandler){
		try{
			RaftResponseMessage response = messageHandler.processRequest(request);
			final ByteBuffer buffer = ByteBuffer.wrap(BinaryUtils.messageToBytes(response));
			connection.write(buffer, null, handlerFrom((Integer bytesSent, Object attachment) -> {
				if(bytesSent.intValue() < buffer.limit()){
					LogManager.getLogger(RpcTcpListener.class).info("failed to completely send the response.");
				}else{
					LogManager.getLogger(RpcTcpListener.class).debug("response message sent.");
				}
			}));
		}catch(Throwable error){
			LogManager.getLogger(RpcTcpListener.class).error("failed to process the request due to error", error);
		}
	}

	private static <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed) {
	    return AsyncUtility.handlerFrom(completed, (Throwable error, A attachment) -> {
	                    LogManager.getLogger(RpcTcpListener.class).info("socket server failure", error);
	                });
	}
}
