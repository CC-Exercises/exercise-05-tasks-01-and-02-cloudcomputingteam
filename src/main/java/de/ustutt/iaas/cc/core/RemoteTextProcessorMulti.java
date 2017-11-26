package de.ustutt.iaas.cc.core;

import java.util.Iterator;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A text processor that sends the text to one of a set of remote REST API for
 * processing (and balances the load between them round-robin).
 * 
 * @author hauptfn
 *
 */
public class RemoteTextProcessorMulti implements ITextProcessor {

	private List<String> textProcessorRessources;
	private Iterator<String> textProcessorRessourcesIterator;
	private Client client;
	private final static Logger logger = LoggerFactory.getLogger(RemoteTextProcessorMulti.class);
	
	public RemoteTextProcessorMulti(List<String> textProcessorResources, Client client) {
		super();
		this.textProcessorRessources = textProcessorResources;
		this.textProcessorRessourcesIterator = textProcessorRessources.iterator();
		this.client = client;
	}

	@Override
	public String process(String text) {
		
		if(!textProcessorRessourcesIterator.hasNext()) {
			textProcessorRessourcesIterator = textProcessorRessources.iterator();
			System.out.println("Lorem!!!");
		} 
		
		String ressource = textProcessorRessourcesIterator.next();		
		WebTarget target = client.target(ressource);
		String processedText = target.request(MediaType.TEXT_PLAIN).post(Entity.entity(text, MediaType.TEXT_PLAIN),
				String.class);
		
		logger.debug("Processed Text - {} - and sent it to the target ressource {}", processedText, ressource);
		
		
		return processedText;
	}

}
