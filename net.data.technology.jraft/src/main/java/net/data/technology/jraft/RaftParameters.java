package net.data.technology.jraft;

public class RaftParameters {

	private int electionTimeoutUpperBound;
	private int electionTimeoutLowerBound;
	private int heartbeatInterval;
	private int rpcFailureBackoff;
	
	public RaftParameters(int electionTimeoutUpper, int electionTimeoutLower, int heartbeatInterval, int rpcFailureBackoff){
		if(heartbeatInterval >= electionTimeoutLower){
			throw new IllegalArgumentException("electionTimeoutLower must be greater than heartbeatInterval");
		}
		this.electionTimeoutLowerBound = electionTimeoutLower;
		this.electionTimeoutUpperBound = electionTimeoutUpper;
		this.heartbeatInterval = heartbeatInterval;
		this.rpcFailureBackoff = rpcFailureBackoff;
	}

	public int getElectionTimeoutUpperBound() {
		return electionTimeoutUpperBound;
	}

	public int getElectionTimeoutLowerBound() {
		return electionTimeoutLowerBound;
	}

	public int getHeartbeatInterval() {
		return heartbeatInterval;
	}

	public int getRpcFailureBackoff() {
		return rpcFailureBackoff;
	}
	
	public int getMaxHeartbeatInterval(){
		return Math.max(this.heartbeatInterval, this.electionTimeoutLowerBound / 2 + this.heartbeatInterval / 2);
	}
}
