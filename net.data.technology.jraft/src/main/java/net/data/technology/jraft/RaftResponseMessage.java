package net.data.technology.jraft;

public class RaftResponseMessage extends RaftMessage {

    private long nextIndex;
    private boolean accepted;

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }
}
