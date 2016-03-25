package net.data.technology.jraft;

public class RpcException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7441647103555107828L;

	private RaftRequestMessage request;
	
	public RpcException(Throwable realException, RaftRequestMessage request){
		super(realException.getMessage(), realException);
		this.request = request;
	}
	
	public RaftRequestMessage getRequest(){
		return this.request;
	}
}
