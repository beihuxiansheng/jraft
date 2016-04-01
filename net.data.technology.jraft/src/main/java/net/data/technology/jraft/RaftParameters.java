package net.data.technology.jraft;

public class RaftParameters {

	private int electionTimeoutUpperBound;
	private int electionTimeoutLowerBound;
	private int heartbeatInterval;
	private int rpcFailureBackoff;
	private int logSyncBatchSize;
	private int logSyncStopGap;
	
	public RaftParameters(int electionTimeoutUpper, int electionTimeoutLower, int heartbeatInterval, int rpcFailureBackoff, int logSyncBatchSize, int logSyncStopGap){
		if(heartbeatInterval >= electionTimeoutLower){
			throw new IllegalArgumentException("electionTimeoutLower must be greater than heartbeatInterval");
		}
		this.electionTimeoutLowerBound = electionTimeoutLower;
		this.electionTimeoutUpperBound = electionTimeoutUpper;
		this.heartbeatInterval = heartbeatInterval;
		this.rpcFailureBackoff = rpcFailureBackoff;
		this.logSyncBatchSize = logSyncBatchSize;
		this.logSyncStopGap = logSyncStopGap;
	}

	/**
	 * Upper value for election timeout
	 * @return
	 */
	public int getElectionTimeoutUpperBound() {
		return electionTimeoutUpperBound;
	}

	/**
	 * Lower value for election timeout
	 * @return
	 */
	public int getElectionTimeoutLowerBound() {
		return electionTimeoutLowerBound;
	}

	/**
	 * Heartbeat interval for each peer
	 * @return
	 */
	public int getHeartbeatInterval() {
		return heartbeatInterval;
	}

	/**
	 * Rpc backoff for peers that failed to be connected
	 * @return
	 */
	public int getRpcFailureBackoff() {
		return rpcFailureBackoff;
	}
	
	/**
	 * The maximum heartbeat interval, any value beyond this may lead to election timeout for a peer before receiving a heartbeat
	 * @return
	 */
	public int getMaxHeartbeatInterval(){
		return Math.max(this.heartbeatInterval, this.electionTimeoutLowerBound / 2 + this.heartbeatInterval / 2);
	}

	/**
	 * The batch size for each ReplicateLogRequest message
	 * @return
	 */
	public int getLogSyncBatchSize() {
		return logSyncBatchSize;
	}

	/**
	 * the max gap allowed for log sync, if the gap between the client and leader is less than this value,
	 * the ReplicateLogRequest will be stopped
	 * @return
	 */
	public int getLogSyncStopGap() {
		return logSyncStopGap;
	}
}
