package net.data.technology.jraft;

public enum LogValueType {

	Application {
		@Override
		public byte toByte(){
			return 1;
		}
	},
	Configuration {
		@Override
		public byte toByte(){
			return 2;
		}
	},
	ClusterServer {
		@Override
		public byte toByte(){
			return 3;
		}
	},
	LogPack {
		@Override
		public byte toByte(){
			return 4;
		}
	};
	
	public abstract byte toByte();
	
	public static LogValueType fromByte(byte b){
		switch(b){
		case 1:
			return Application;
		case 2:
			return Configuration;
		case 3:
			return ClusterServer;
		case 4:
			return LogPack;
		default:
			throw new IllegalArgumentException(String.format("%d is not defined for LogValueType", b));
		}
	}
}
