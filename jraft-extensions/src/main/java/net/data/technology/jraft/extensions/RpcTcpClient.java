package net.data.technology.jraft.extensions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcClient;

public class RpcTcpClient implements RpcClient {

	private AsynchronousSocketChannel connection;
	private InetSocketAddress remote;
	private Logger logger;
	
	public RpcTcpClient(InetSocketAddress remote){
		this.remote = remote;
		this.logger = LogManager.getLogger(getClass());
	}
	
	@Override
	public synchronized CompletableFuture<RaftResponseMessage> send(final RaftRequestMessage request) {
		this.logger.debug(String.format("trying to send message %s to server %d at endpoint %s", request.getMessageType().toString(), request.getDestination(), this.remote.toString()));
		final CompletableFuture<RaftResponseMessage> result = new CompletableFuture<RaftResponseMessage>();
		if(this.connection == null || !this.connection.isOpen()){
			try{
				this.connection = AsynchronousSocketChannel.open();
				this.connection.connect(this.remote, null, handlerFrom((Void v, Object attachment) -> {
					sendAndRead(request, result);
				}, result));
			}catch(Throwable error){
				closeSocket();
				result.completeExceptionally(error);
			}
		}else{
			this.sendAndRead(request, result);
		}
		
		return result;
	}
	
	private void sendAndRead(RaftRequestMessage request, CompletableFuture<RaftResponseMessage> future){
		ByteBuffer buffer = ByteBuffer.wrap(BinaryUtils.messageToBytes(request));
		try{
			this.connection.write(buffer, null, handlerFrom((Integer bytesSent, Object attachment) -> {
				if(bytesSent.intValue() < buffer.limit()){
					logger.info("failed to sent the request to remote server.");
					future.completeExceptionally(new IOException("Only partial of the data could be sent"));
					closeSocket();
				}else{
					// read the response
					ByteBuffer responseBuffer = ByteBuffer.allocate(BinaryUtils.RAFT_RESPONSE_HEADER_SIZE);
					try{
						this.connection.read(responseBuffer, null, handlerFrom((Integer bytesRead, Object state) -> {
							if(bytesRead.intValue() < BinaryUtils.RAFT_RESPONSE_HEADER_SIZE){
								logger.info("failed to read response from remote server.");
								future.completeExceptionally(new IOException("Only part of the response data could be read"));
								closeSocket();
							}else{
								future.complete(BinaryUtils.bytesToResponseMessage(responseBuffer.array()));
							}
						}, future));
					}catch(Exception readError){
						logger.info("failed to read from socket", readError);
						future.completeExceptionally(readError);
						closeSocket();
					}
				}
			}, future));
		}catch(Exception writeError){
			logger.info("failed to write the socket", writeError);
			future.completeExceptionally(writeError);
			closeSocket();
		}
	}
	
	private <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed, CompletableFuture<RaftResponseMessage> future) {
	    return AsyncUtility.handlerFrom(completed, (Throwable error, A attachment) -> {
	                    future.completeExceptionally(error);
	                    closeSocket();
	                });
	}
	
	private synchronized void closeSocket(){
		this.logger.debug("close the socket due to errors");
		try{
			if(this.connection != null){
				this.connection.close();
				this.connection = null;
			}
    	}catch(IOException ex){
    		this.logger.info("failed to close socket", ex);
    	}
	}
}
