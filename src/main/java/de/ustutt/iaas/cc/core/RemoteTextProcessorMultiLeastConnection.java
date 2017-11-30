package de.ustutt.iaas.cc.core;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteTextProcessorMultiLeastConnection implements ITextProcessor {

	private Client client;
	// TODO Make queue Concurrent!
	private CopyOnWriteArrayList<Endpoint> endpointList;
	
	private final static Logger logger = LoggerFactory.getLogger(RemoteTextProcessorMultiLeastConnection.class);

	public RemoteTextProcessorMultiLeastConnection(List<String> eps, Client clientm) {
		super();
		this.client = clientm;
		this.endpointList = new CopyOnWriteArrayList<Endpoint>();
		eps.forEach(ep -> endpointList.add(new Endpoint(ep)));
	}

	@Override
	public String process(String text) { 
		synchronized (this) {
			logger.debug(endpointList.toString());
			Collections.sort(endpointList);
		}		
		Endpoint endpoint = endpointList.get(0); // Use poll to resort heap
		logger.debug("-----------------------------------");
		logger.debug("Open Connections:" + endpoint.openConnections);
		logger.debug("Using resource:" + endpoint.resource);
		logger.debug("-----------------------------------");
		endpoint.openConnections ++;
		WebTarget target = client.target(endpoint.resource);
		String processedText = target.request(MediaType.TEXT_PLAIN).post(Entity.entity(text, MediaType.TEXT_PLAIN),
				String.class);
		
		logger.debug("Processed Text - {} - and sent it to the target ressource {}", processedText, endpoint.resource);
		endpoint.openConnections--;
		
		return processedText;
	}
	
	private class Endpoint implements Comparable<Endpoint>{
		
		public Endpoint(String ep) {
			this.resource = ep;
		}
		
		private String resource;
		private int openConnections;
		
		@Override
		public int compareTo(Endpoint o) {
			if (this.openConnections < o.openConnections) {
				return -1;
			} else if (this.openConnections > o.openConnections) {
				return 1;
			}
			return 0;
		}
		
		@Override
		public String toString() {
			return "Resource: " + this.resource + ", Open Connections: " + this.openConnections;
		}
	}

}
