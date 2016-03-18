package net.data.technology.jraft;

class RequestContext {
	private int totalServers;
	private Object state;
	
	public RequestContext(int totalServers, Object state){
		this.totalServers = totalServers;
		this.state = state;
	}
	
	public RequestContext(int totalServers){
		this(totalServers, null);
	}

	public int getTotalServers() {
		return totalServers;
	}

	public Object getState() {
		return state;
	}
}
