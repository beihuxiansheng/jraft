package net.data.technology.jraft;

import java.util.Calendar;
import java.util.Random;

import org.apache.log4j.LogManager;

public class DummyMessageHandler implements RaftMessageHandler {

    private Random random = new Random(Calendar.getInstance().getTimeInMillis());
    private org.apache.log4j.Logger logger = LogManager.getLogger(getClass());

    @Override
    public RaftResponseMessage processRequest(RaftRequestMessage request) {
        String log = String.format(
                "Receive a request(Source: %d, Destination: %d, Term: %d, LLI: %d, LLT: %d, CI: %d, LEL: %d",
                request.getSource(),
                request.getDestination(),
                request.getTerm(),
                request.getLastLogIndex(),
                request.getLastLogTerm(),
                request.getCommitIndex(),
                request.getLogEntries() == null ? 0 : request.getLogEntries().length);
        logger.debug(log);
        System.out.println(log);
        return this.randomResponse(request.getSource(), request.getTerm());
    }

    private RaftMessageType randomMessageType(){
        byte value = (byte)this.random.nextInt(5);
        return RaftMessageType.fromByte((byte) (value + 1));
    }

    private RaftResponseMessage randomResponse(int source, long term){
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(this.randomMessageType());
        response.setAccepted(this.random.nextBoolean());
        response.setDestination(source);
        response.setSource(this.random.nextInt());
        response.setTerm(term);
        response.setNextIndex(this.random.nextLong());
        return response;
    }
}
