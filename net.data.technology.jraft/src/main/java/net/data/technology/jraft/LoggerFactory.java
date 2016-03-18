package net.data.technology.jraft;

public interface LoggerFactory {

	public Logger getLogger(Class<?> clazz);
}
