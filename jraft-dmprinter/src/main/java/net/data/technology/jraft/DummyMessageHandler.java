package net.data.technology.jraft;

import java.util.Calendar;
import java.util.Random;

public class DummyMessageHandler implements RaftMessageHandler {

	private Random random = new Random(Calendar.getInstance().getTimeInMillis());
	
	@Override
	public RaftResponseMessage processRequest(RaftRequestMessage request) {
		System.out.println(
				String.format(
						"Receive a request(Source: %d, Destination: %d, Term: %d, LLI: %d, LLT: %d, CI: %d, LEL: %d",
						request.getSource(),
						request.getDestination(),
						request.getTerm(),
						request.getLastLogIndex(),
						request.getLastLogTerm(),
						request.getCommitIndex(),
						request.getLogEntries() == null ? 0 : request.getLogEntries().length));
		return this.randomResponse();
	}

	private RaftMessageType randomMessageType(){
		byte value = (byte)this.random.nextInt(5);
		return RaftMessageType.fromByte((byte) (value + 1));
	}
	
	private RaftResponseMessage randomResponse(){
		RaftResponseMessage response = new RaftResponseMessage();
		response.setMessageType(this.randomMessageType());
		response.setAccepted(this.random.nextBoolean());
		response.setDestination(this.random.nextInt());
		response.setSource(this.random.nextInt());
		response.setTerm(this.random.nextLong());
		response.setNextIndex(this.random.nextLong());
		return response;
	}
}
