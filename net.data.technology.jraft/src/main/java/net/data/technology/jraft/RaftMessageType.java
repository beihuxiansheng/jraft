package net.data.technology.jraft;

public enum RaftMessageType {

	RequestVoteRequest {
		@Override
		public String toString() {
			return "RequestVoteRequest";
		}

		@Override
		public byte toByte() {
			return (byte) 1;
		}
	},
	RequestVoteResponse {
		@Override
		public String toString() {
			return "RequestVoteResponse";
		}

		@Override
		public byte toByte() {
			return (byte) 2;
		}
	},
	AppendEntriesRequest {
		@Override
		public String toString() {
			return "AppendEntriesRequest";
		}

		@Override
		public byte toByte() {
			return (byte) 3;
		}
	},
	AppendEntriesResponse {
		@Override
		public String toString() {
			return "AppendEntriesResponse";
		}

		@Override
		public byte toByte() {
			return (byte) 4;
		}
	},
	ClientRequest {
		@Override
		public String toString() {
			return "ClientRequest";
		}

		@Override
		public byte toByte() {
			return (byte) 5;
		}
	};

	public abstract byte toByte();

	public static RaftMessageType fromByte(byte value) {
		switch (value) {
		case 1:
			return RequestVoteRequest;
		case 2:
			return RequestVoteResponse;
		case 3:
			return AppendEntriesRequest;
		case 4:
			return AppendEntriesResponse;
		case 5:
			return ClientRequest;
		}

		return null;
	}
}
