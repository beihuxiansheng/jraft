package net.data.technology.jraft.extensions;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.BiConsumer;

public class AsyncUtility {

	public static <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed, BiConsumer<Throwable, A> failed) {
	    return new CompletionHandler<V, A>() {
	        @Override
	        public void completed(V result, A attachment) {
	            completed.accept(result, attachment);
	        }

	        @Override
	        public void failed(Throwable exc, A attachment) {
	            failed.accept(exc, attachment);
	        }
	    };
	}
	
	public static <A> void readFromChannel(AsynchronousByteChannel channel, ByteBuffer buffer, A attachment, CompletionHandler<Integer, A> completionHandler){
		try{
			channel.read(
					buffer,
					attachment, 
					handlerFrom(
					(Integer result, A a) -> {
						int bytesRead = result.intValue();
						if(bytesRead == -1 || !buffer.hasRemaining()){
							completionHandler.completed(buffer.position(), attachment);
						}else{
							readFromChannel(channel, buffer, attachment, completionHandler);
						}
					}, 
					(Throwable error, A a) -> {
						completionHandler.failed(error, attachment);
					}));
		}catch(Throwable exception){
			completionHandler.failed(exception, attachment);
		}
	}
	
	public static <A> void writeToChannel(AsynchronousByteChannel channel, ByteBuffer buffer, A attachment, CompletionHandler<Integer, A> completionHandler){
		try{
			channel.write(
					buffer,
					attachment, 
					handlerFrom(
					(Integer result, A a) -> {
						int bytesRead = result.intValue();
						if(bytesRead == -1 || !buffer.hasRemaining()){
							completionHandler.completed(buffer.position(), attachment);
						}else{
							writeToChannel(channel, buffer, attachment, completionHandler);
						}
					}, 
					(Throwable error, A a) -> {
						completionHandler.failed(error, attachment);
					}));
		}catch(Throwable exception){
			completionHandler.failed(exception, attachment);
		}
	}
}
