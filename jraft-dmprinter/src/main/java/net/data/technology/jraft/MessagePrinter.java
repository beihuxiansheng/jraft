package net.data.technology.jraft;

import java.io.UnsupportedEncodingException;

public class MessagePrinter implements StateMachine {

	@Override
	public void commit(long logIndex, byte[] data) {
		try {
			String message = new String(data, "utf-8");
			System.out.printf("commit: %d\t%s\n", logIndex, message);
		} catch (UnsupportedEncodingException e) {
			System.out.println("failed to convert data to String due to error " + e.getMessage());
		}
	}

}
