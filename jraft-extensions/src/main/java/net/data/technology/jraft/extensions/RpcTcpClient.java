package net.data.technology.jraft.extensions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcClient;

public class RpcTcpClient implements RpcClient {

	private AsynchronousSocketChannel connection;
	private ConcurrentLinkedQueue<ReadTask> readTasks;
	private AtomicInteger readers;
	private InetSocketAddress remote;
	private Logger logger;
	
	public RpcTcpClient(InetSocketAddress remote){
		this.remote = remote;
		this.logger = LogManager.getLogger(getClass());
		this.readTasks = new ConcurrentLinkedQueue<ReadTask>();
		this.readers = new AtomicInteger(0);
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
			AsyncUtility.writeToChannel(this.connection, buffer, null, handlerFrom((Integer bytesSent, Object attachment) -> {
				if(bytesSent.intValue() < buffer.limit()){
					logger.info("failed to sent the request to remote server.");
					future.completeExceptionally(new IOException("Only partial of the data could be sent"));
					closeSocket();
				}else{
					// read the response
					ByteBuffer responseBuffer = ByteBuffer.allocate(BinaryUtils.RAFT_RESPONSE_HEADER_SIZE);
					CompletionHandler<Integer, Object> handler = handlerFrom((Integer bytesRead, Object state) -> {
						if(bytesRead.intValue() < BinaryUtils.RAFT_RESPONSE_HEADER_SIZE){
							logger.info("failed to read response from remote server.");
							future.completeExceptionally(new IOException("Only part of the response data could be read"));
							closeSocket();
						}else{
							future.complete(BinaryUtils.bytesToResponseMessage(responseBuffer.array()));
						}
						
						int waitingReaders = this.readers.decrementAndGet();
						if(waitingReaders > 0){
							this.logger.debug("there are pending readers in queue, will try to process them");
							ReadTask task = this.readTasks.poll();
							if(task != null){
								this.readResponse(task.buffer, task.completionHandler, task.future);
							}else{
								this.logger.error("there are pending readers but get a null ReadTask");
							}
						}
					}, future);
					int readerCount = this.readers.getAndIncrement();
					if(readerCount > 0){
						this.logger.debug("there is a pending read, queue this read task");
						this.readTasks.add(new ReadTask(responseBuffer, handler, future));
					}else{
						this.readResponse(responseBuffer, handler, future);
					}
				}
			}, future));
		}catch(Exception writeError){
			logger.info("failed to write the socket", writeError);
			future.completeExceptionally(writeError);
			closeSocket();
		}
	}
	
	private void readResponse(ByteBuffer responseBuffer, CompletionHandler<Integer, Object> handler, CompletableFuture<RaftResponseMessage> future){
		try{
			this.logger.debug("reading response from socket...");
			AsyncUtility.readFromChannel(this.connection, responseBuffer, null, handler);
		}catch(Exception readError){
			logger.info("failed to read from socket", readError);
			future.completeExceptionally(readError);
			closeSocket();
		}
	}
	
	private <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed, CompletableFuture<RaftResponseMessage> future) {
	    return AsyncUtility.handlerFrom(completed, (Throwable error, A attachment) -> {
	    				this.logger.info("socket error", error);
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
		
		while(true){
			ReadTask task = this.readTasks.poll();
			if(task == null){
				break;
			}
			
			task.future.completeExceptionally(new IOException("socket is closed, no more reads can be completed"));
		}

		this.readers.set(0);
	}
	
	static class ReadTask{
		private ByteBuffer buffer;
		private CompletionHandler<Integer, Object> completionHandler;
		private CompletableFuture<RaftResponseMessage> future;
		
		ReadTask(ByteBuffer buffer, CompletionHandler<Integer, Object> completionHandler, CompletableFuture<RaftResponseMessage> future){
			this.buffer = buffer;
			this.completionHandler = completionHandler;
			this.future = future;
		}
	}
}
