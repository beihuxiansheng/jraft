package net.data.technology.jraft;

public class ServerState {

	private long term;
	private long commitIndex;
	private int votedFor;
	
	public long getTerm() {
		return term;
	}

	public void setTerm(long term) {
		this.term = term;
	}

	public int getVotedFor() {
		return votedFor;
	}

	public void setVotedFor(int votedFor) {
		this.votedFor = votedFor;
	}
	
	public void increaseTerm(){
		this.term += 1;
	}

	public long getCommitIndex() {
		return commitIndex;
	}

	public void setCommitIndex(long commitIndex) {
		if(commitIndex > this.commitIndex){
			this.commitIndex = commitIndex;
		}
	}
}
