package dhl.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.apama.epl.plugin.annotation.*;
import com.apama.jmon.annotation.*;
import com.apama.util.Logger;

@Application(name = "KafkaPlugin", 
	author = "", 
	version = "1.0", 
	company = "", 
	description = "", 
	classpath = "lib/kafka-clients-2.0.0.jar;lib/snappy-java-1.1.7.1.jar")                


@EPLPlugin(description = "Manages kafka consumer and producer", name= "dhl.demo.KafkaPlugin")
public class KafkaPlugin {

	private static Logger logger = Logger.getLogger();

	private static Map<String, TopicConsumerRunnable> topicConsumers = new ConcurrentHashMap<String, TopicConsumerRunnable>();

	private static Map<String, Boolean> consumersStarted = new ConcurrentHashMap<String, Boolean>();

	private static Map<String, TopicProducer> topicProducers = new ConcurrentHashMap<String, TopicProducer>();

	private static String bootstrapServers;

	private static boolean initialised;
	private static boolean started;
	
	public static boolean isInitialised() {
		return initialised;
	}

	public static boolean isStarted() {
		return started;
	}
	
	public static void initialise(String bootstrapServers) {
		KafkaPlugin.bootstrapServers = bootstrapServers;
		
		initialised = true;
		started = false;
						
		logger.info("Kafka plugin initialised, bootstrap servers = "+bootstrapServers);
	}
	
	public static boolean addKafkaTopicProducer(String topic, String codeTopicMapping) {
		if (initialised) {
			if (!topicProducers.containsKey(codeTopicMapping)) {
				try {
					TopicProducer<String, String> producer = new TopicProducer<String, String>(bootstrapServers, topic);
					topicProducers.put(codeTopicMapping, producer);
					logger.info("Created and added Kafka producer on topic = "+topic+" with code topic mapping = "+codeTopicMapping);
					return true;
				}
				catch (Exception ex) {
					logger.error("Issue creating producer for "+topic, ex);
				}
			} else {
				logger.warn("Not creating Kafka producer as producer already exists on topic = "+topic+" using code topic mapping "+codeTopicMapping);
			}
		} else {
			logger.error("Not creating Kafka producer as initialisation has not been received");
		}
		return false;
	}

	public static boolean addKafkaTopicConsumer(String topic, String consumerGroup) {
		if (initialised) {
			if (!topicConsumers.containsKey(topic)) {
				TopicConsumerRunnable tcr = new TopicConsumerRunnable(bootstrapServers, topic, consumerGroup, true);

				topicConsumers.put(topic, tcr);
				consumersStarted.put(topic, false);
				logger.info("Added Kafka consumer on topic = "+topic);
				return true;
			} else {
				logger.warn("Not creating Kafka consumer as consumer already exists on topic = "+topic);
			}
		} else {
			logger.error("Not creating Kafka consumer as initialisation has not been received");
		}
		return false;
	}

	public static boolean publishMessage(String topic, String value, boolean block) {
		if (initialised) {
			if (topicProducers.containsKey(topic)) {
				TopicProducer<String, String> topicProducer = topicProducers.get(topic);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicProducer.getTopic(), value);
				return topicProducer.publish(record, block);
			}
			else {
				logger.warn("Can not send message as no producer found for code mapped topic = "+topic);
			}
		}
		return false;
	}
	
	public static void start() {
		if ( !started ) {
			
			logger.info("Starting Kafka consumers");
			for(String topicName : consumersStarted.keySet()) {
				if (!consumersStarted.get(topicName) && topicConsumers.containsKey(topicName)) {
					logger.info("Attempting to start consumer for topic = "+topicName);
					new Thread(topicConsumers.get(topicName)).start();
					consumersStarted.put(topicName, true);
				}
			}
			
			started = true;
		}
	}

	private static void stopConsumers() throws Exception {
		logger.info("Stopping Kafka Consumers");
		for (TopicConsumerRunnable consumer : topicConsumers.values()) {
			consumer.stopConsumer();
		}
	}
	
	private static void stopPublishers() {
		logger.info("Stopping Kafka Publishers");
		for (TopicProducer producer : topicProducers.values()) {
			producer.stop();
		}
	}
	
	@com.apama.epl.plugin.annotation.Callback(type=com.apama.epl.plugin.annotation.Callback.CBType.SHUTDOWN)
	public static void shutdown() {
		logger.info("Kafka adapter is shutting down");
		try {
			stopPublishers();
			stopConsumers();
		} catch(Exception ex) {
			logger.error("Caught exception when shutting down KafkaAdapter", ex);
		}
	}
}
