package de.ustutt.iaas.cc.core;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncReceiver implements MessageListener, ExceptionListener{

	final Logger logger = LoggerFactory.getLogger(AsyncReceiver.class);
	
	private QueueTextProcessor processor;
	
	public AsyncReceiver(QueueTextProcessor processor) {
		this.processor = processor;
	}
	
	@Override
	public void onMessage(Message msg) {
		try {
			logger.debug("Received message: {}", msg);
			
			// Check if message is from NotebookApp
			@SuppressWarnings("unchecked")
			boolean appPropertyExists = Collections.list(msg.getPropertyNames())
					.stream().anyMatch(e -> ((String) e).equals("app"));
			if(!appPropertyExists) {
				return;
			}
			
			logger.info("- App = {}", msg.getObjectProperty("App"));
			String cId = (String) msg.getObjectProperty("cid");
			logger.info("- CorelationIdentifier = {}", cId);
			UUID id = UUID.fromString(cId);
			
			if (msg instanceof TextMessage) {
				TextMessage textMsg = (TextMessage) msg;
				String response = textMsg.getText();
				
				CompletableFuture<String> futureString = processor.getResponseMap().get(id);
				futureString.complete(response);
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public void onException(JMSException e) {
		e.printStackTrace();
		
	}

}
