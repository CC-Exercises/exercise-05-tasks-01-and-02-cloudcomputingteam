package de.ustutt.iaas.cc.core;

import java.util.List;

import javax.ws.rs.client.Client;

public class RemoteTextProcessorMultiChainedFailover implements ITextProcessor {

	public RemoteTextProcessorMultiChainedFailover(List<String> eps, Client clientm) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String process(String text) {
		// TODO Auto-generated method stub
		return "Sheduled and not routet by ChainedFailoverProcessor";
	}

}
