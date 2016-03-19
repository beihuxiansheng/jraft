package net.data.technology.jraft.extensions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcClient;

public class RpcTcpClient implements RpcClient {

	private AsynchronousSocketChannel connection;
	private InetSocketAddress remote;
	
	public RpcTcpClient(InetSocketAddress remote){
		this.remote = remote;
	}
	
	@Override
	public CompletableFuture<RaftResponseMessage> send(final RaftRequestMessage request) {
		final CompletableFuture<RaftResponseMessage> result = new CompletableFuture<RaftResponseMessage>();
		if(this.connection == null){
			try{
				this.connection = AsynchronousSocketChannel.open();
				this.connection.connect(this.remote, null, handlerFrom((Void v, Object attachment) -> {
					sendAndRead(request, result);
				}, result));
			}catch(Throwable error){
				result.completeExceptionally(error);
			}
		}else{
			this.sendAndRead(request, result);
		}
		
		return result;
	}
	
	private void sendAndRead(RaftRequestMessage request, CompletableFuture<RaftResponseMessage> future){
		ByteBuffer buffer = ByteBuffer.wrap(BinaryUtils.messageToBytes(request));
		this.connection.write(buffer, null, handlerFrom((Integer bytesSent, Object attachment) -> {
			if(bytesSent.intValue() < buffer.limit()){
				LogManager.getLogger(RpcTcpClient.class).info("failed to sent the request to remote server.");
				future.completeExceptionally(new IOException("Only partial of the data could be sent"));
			}else{
				// read the response
				ByteBuffer responseBuffer = ByteBuffer.allocate(BinaryUtils.RAFT_RESPONSE_HEADER_SIZE);
				this.connection.read(responseBuffer, null, handlerFrom((Integer bytesRead, Object state) -> {
					if(bytesRead.intValue() < BinaryUtils.RAFT_RESPONSE_HEADER_SIZE){
						LogManager.getLogger(RpcTcpClient.class).info("failed to read response from remote server.");
						future.completeExceptionally(new IOException("Only part of the response data could be read"));
					}else{
						future.complete(BinaryUtils.bytesToResponseMessage(responseBuffer.array()));
					}
				}, future));
			}
		}, future));
	}
	
	private static <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed, CompletableFuture<RaftResponseMessage> future) {
	    return AsyncUtility.handlerFrom(completed, (Throwable error, A attachment) -> {
	                    future.completeExceptionally(error);
	                });
	}
}
