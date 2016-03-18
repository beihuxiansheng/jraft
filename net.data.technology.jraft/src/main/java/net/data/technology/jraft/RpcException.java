package net.data.technology.jraft;

public class RpcException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7441647103555107828L;

	public RpcException(Throwable realException){
		super(realException.getMessage(), realException);
	}
}
