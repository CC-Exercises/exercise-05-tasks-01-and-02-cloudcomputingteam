package de.ustutt.iaas.cc.core;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import de.ustutt.iaas.cc.TextProcessorConfiguration;

/**
 * A text processor that uses JMS to send text to a request queue and then waits
 * for the processed text on a response queue. For each text processing request,
 * a unique ID is generated that is later used to correlate responses to their
 * original request.
 * <p>
 * The text processing is realized by (one or more) workers that read from the
 * request queue and write to the response queue.
 * <p>
 * This implementation supports ActiveMQ as well as AWS SQS.
 * 
 * @author hauptfn
 *
 */
public class QueueTextProcessor implements ITextProcessor {

	final Logger logger = LoggerFactory.getLogger(QueueTextProcessor.class);

	private QueueConnectionFactory conFactory;
	private QueueConnection connection;
	private QueueSession session;
	private Queue textProcessorRequests;
	private Queue textProcessorResponses;
	private QueueSender textProcessorRequestSender;
	private QueueReceiver textProcessorResponseReceiver;
	private AsyncReceiver asyncReceiver;
	
	private ConcurrentHashMap<UUID, CompletableFuture<String>> responseMap ;

	private final String REQUEST_QUEUE_NAME = "text-processor-requests";
	private final String RESPONSE_QUEUE_NAME = "text-processor-responses";
	private final String AWS_CREDENTIALS = "aws.properties";

	public QueueTextProcessor(TextProcessorConfiguration conf) {
		super();		
		logger.debug("Initializing QueueTextProcessor.");
		
		this.responseMap = new ConcurrentHashMap<UUID, CompletableFuture<String>>();
		
		try {
			conFactory = SQSConnectionFactory.builder().withRegion(Region.getRegion(Regions.US_WEST_2))
					.withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(AWS_CREDENTIALS)).build();

			connection = conFactory.createQueueConnection();
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			textProcessorRequests = session.createQueue(REQUEST_QUEUE_NAME);
			textProcessorResponses = session.createQueue(RESPONSE_QUEUE_NAME);
			textProcessorRequestSender = session.createSender(textProcessorRequests);
			textProcessorResponseReceiver = session.createReceiver(textProcessorResponses);
			asyncReceiver = new AsyncReceiver(this);
			textProcessorResponseReceiver.setMessageListener(asyncReceiver);
			connection.setExceptionListener(asyncReceiver);
			connection.start();
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public String process(String text) {
		String processedText = "";
		try {
			TextMessage message = session.createTextMessage(text);
			message.setStringProperty("app", "My first Notebook App");
			UUID id = UUID.randomUUID();
			message.setStringProperty("cid", id.toString());
			textProcessorRequestSender.send(message);
			
			//Async Receive Call
			CompletableFuture<String> futureResponse = new CompletableFuture<String>();
			responseMap.put(id, futureResponse);
			
			processedText = futureResponse.get();
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return processedText;
	}
	
	public ConcurrentHashMap<UUID, CompletableFuture<String>> getResponseMap() {
		return responseMap;
	}

}
