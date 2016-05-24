package net.data.technology.jraft;

public interface Logger {

	public void debug(String format, Object... args);
	
	public void info(String format, Object... args);
	
	public void warning(String format, Object... args);
	
	public void error(String format, Object... args);
	
	public void error(String format, Throwable error, Object... args);
}
