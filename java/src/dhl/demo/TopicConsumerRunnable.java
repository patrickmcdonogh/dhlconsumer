package dhl.demo;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import com.apama.util.Logger;

public class TopicConsumerRunnable implements Runnable {

	final static Logger logger = Logger.getLogger();

	private String bootstrapServers;
	private String topic; 
	private String consumerGroup;
	private boolean autoAck;
	
	private final AtomicBoolean stopped;

	private CountDownLatch shutdownLatch;

	private int messagesReceived;
	
	private final KafkaMessageHandler kafkaMessageHandler;
	
	public TopicConsumerRunnable(String bootstrapServers, String topic, String consumerGroup, boolean autoAck) {
		super();
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
		this.consumerGroup = consumerGroup;
		this.autoAck = autoAck;
		
		this.kafkaMessageHandler = new MessageHandler();
		
		stopped = new AtomicBoolean(false);
		messagesReceived = 0;

		logger.info("Creating kafka consumer. Topic: " + topic + ", Group: " + consumerGroup + ", Auto ACK: " + autoAck + " handler class:" + kafkaMessageHandler.getClass().getCanonicalName());
		logger.info("Contexts target number of contexts: ");
	}

	private void createConsumer() {

		stopped.set(false);
		shutdownLatch = new CountDownLatch(1);

		kafkaMessageHandler.setupKafkaConsumer(bootstrapServers, topic, consumerGroup, autoAck, new RebalanceManager());
	}

	private class RebalanceManager implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

			for (TopicPartition part : partitions) {
				logger.info(topic+" revoking partition "+part.partition());
			}
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			for (TopicPartition part : partitions) {
				logger.info(topic+" assigning partition "+part.partition());
			}
		}
	}

	@Override
	public void run() {
		createConsumer();

		logger.info("Started Kafka subscriber on topic = "+topic+" using group = "+consumerGroup+" and auto commit = "+autoAck);
		int recordCount = 0;
		try {
			while(!stopped.get()) {
				try {
					recordCount = kafkaMessageHandler.pollAndHandleMessages();
					if (recordCount > 0 ) {
						messagesReceived+=recordCount;
						logger.info("Received "+recordCount+" from "+topic+" total received = "+messagesReceived);
					}
				}
				catch(WakeupException ex) {
					logger.info("Wake up on "+topic);
				}
				catch(Exception ex) {
					logger.error("Error while polling and processing messages on "+topic, ex);
				}
			}
		}
		catch(Exception ex) {
			// Called if an exception is thrown
			logger.error("Exception thrown when consuming from "+topic, ex);
		}
		finally {
			logger.info("Stopping Kafka subscriber on topic = "+topic);
			try {
				kafkaMessageHandler.terminatePoll();
			} catch (Exception ex) {
				logger.error("Issue unsubscribing and closing from topic "+topic, ex);
			}

			shutdownLatch.countDown();
		}
	}

	public void stopConsumer() throws InterruptedException {
		stopped.set(true);
		logger.info("Waiting for shutdown latch on "+topic);
		shutdownLatch.await();
		logger.info("Shutdown latch completed on "+topic);
	}
}