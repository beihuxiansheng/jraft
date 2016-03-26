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
	},
	AddServerRequest {
		@Override
		public String toString() {
			return "AddServerRequest";
		}

		@Override
		public byte toByte() {
			return (byte) 6;
		}
	},
	AddServerResponse {
		@Override
		public String toString() {
			return "AddServerResponse";
		}

		@Override
		public byte toByte() {
			return (byte) 7;
		}
	},
	RemoveServerRequest {
		@Override
		public String toString(){
			return "RemoveServerRequest";
		}
		
		@Override
		public byte toByte(){
			return (byte)8;
		}
	},
	RemoveServerResponse {
		@Override
		public String toString(){
			return "RemoveServerResponse";
		}
		
		@Override
		public byte toByte(){
			return (byte)9;
		}
	},
	SyncLogRequest {
		@Override
		public String toString(){
			return "SyncLogRequest";
		}
		
		@Override
		public byte toByte(){
			return (byte)10;
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
		case 6:
			return AddServerRequest;
		case 7:
			return AddServerResponse;
		case 8:
			return RemoveServerRequest;
		case 9:
			return RemoveServerResponse;
		case 10:
			return SyncLogRequest;
		}

		throw new IllegalArgumentException("the value for the message type is not defined");
	}
}
