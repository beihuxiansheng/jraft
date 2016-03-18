package net.data.technology.jraft.extensions;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.Logger;
import net.data.technology.jraft.LoggerFactory;

public class Log4jLoggerFactory implements LoggerFactory {

	public Logger getLogger(Class<?> clazz) {
		return new Log4jLogger(LogManager.getLogger(clazz));
	}

}
