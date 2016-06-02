package net.data.technology.jraft.extensions.http;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import net.data.technology.jraft.LoggerFactory;
import net.data.technology.jraft.RaftConsensus;
import net.data.technology.jraft.RaftContext;
import net.data.technology.jraft.RaftMessageHandler;
import net.data.technology.jraft.RaftMessageSender;
import net.data.technology.jraft.RaftParameters;
import net.data.technology.jraft.RpcListener;
import net.data.technology.jraft.ServerStateManager;
import net.data.technology.jraft.StateMachine;
import net.data.technology.jraft.extensions.Log4jLoggerFactory;

public abstract class JraftServletListener implements RpcListener, ServletContextListener {

	public static final String JRAFT_MESSAGE_SENDER = "$Jraft$Message$Sender";
	public static final String JRAFT_MESSAGE_HANDLER = "$Jraft$Message$Handler";
	
	private ServletContext servletContext;
	
	public static RaftMessageHandler getMessageHandler(ServletContext context){
		return (RaftMessageHandler)context.getAttribute(JRAFT_MESSAGE_HANDLER);
	}
	
	public static RaftMessageSender getMessageSender(ServletContext context){
		return (RaftMessageSender)context.getAttribute(JRAFT_MESSAGE_SENDER);
	}
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		this.servletContext = sce.getServletContext();
		RaftContext context = new RaftContext(
				this.getServerStateManager(),
				this.getStateMachine(), 
				this.getParameters(),
				this, 
				this.getLoggerFactory(), 
				new HttpRpcClientFactory());
		this.servletContext.setAttribute(JRAFT_MESSAGE_SENDER, RaftConsensus.run(context));
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
	}

	@Override
	public void startListening(RaftMessageHandler messageHandler) {
		this.servletContext.setAttribute(JRAFT_MESSAGE_HANDLER, messageHandler);
	}

	@Override
	public abstract void stop();
	
	protected abstract RaftParameters getParameters();
	
	protected abstract ServerStateManager getServerStateManager();
	
	protected abstract StateMachine getStateMachine();
	
	protected LoggerFactory getLoggerFactory(){
		return new Log4jLoggerFactory();
	}

}
