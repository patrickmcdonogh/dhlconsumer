package dhl.demo;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;

import com.apama.util.Logger;

public class TopicProducer<K,V>  {
	
	protected static Logger logger = Logger.getLogger();
	
	protected String bootstrapServers;
	protected String topic;
	
	protected KafkaProducer<K, V> producer;
	private BlockingQueue<ProducerRecord<K, V>> messageQueue;
	
	private int messagesPublished;
	
	private final AtomicBoolean running = new AtomicBoolean(true);
	
	public TopicProducer(String bootstrapServers, String topic) {
		super();
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
		createPublisher();
	}

	public String getTopic() {
		return topic;
	}
	
	private void createPublisher() {
        //switch classloader so that kafka can find the serializers
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);

		producer = createProducer();
		logger.info("Created producer for " + topic);

        Thread.currentThread().setContextClassLoader(original);
        
        messageQueue = new ArrayBlockingQueue<>(200 * 1000);
        
        new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(running.get()) {
					try {
						sendRecord((ProducerRecord<K, V>)messageQueue.take());
				        messagesPublished++;
					}
					catch(Exception ex) {
						logger.error("Error sending data to Kafka topic = "+topic, ex);
					}
				}
				
				logger.info("Closing producer on topic = "+topic);
				producer.close();
				
			}
		}).start();
	}

	protected KafkaProducer<K, V> createProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000");

		return new KafkaProducer<K, V>(properties);
	}

	protected void sendRecord(ProducerRecord<K, V> record) {		
		ProducerRecord<K, V> theRecord = (ProducerRecord<K, V>)record;
			
		producer.send(theRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata arg0, Exception ex) {
				if (ex != null) {
					logger.error("Error sending message to Kafka topic = "+topic, ex);
					if (ex instanceof RecordTooLargeException) {
						if (theRecord.value() instanceof String) {
							logger.error("Too Large Message:" + ((String)theRecord.value()).substring(0,500));
						}
						
					}
					else {
						logger.error("Message:" + record.value());
					}
				}
			}
		});
	}
	
	public void stop() {
		running.set(false);
	}
	
	public boolean publish(ProducerRecord<K, V> record, boolean block) {
		boolean output = true;
		try {
			if ( block ) {
				messageQueue.put(record);
			}
			else
			{				
				output = messageQueue.offer(record);
		        if ( !output ) {
	        	logger.error("Could not add message to internal queue for Kafka topic = "+topic+". Internal message queue size = "+messageQueue.size());
	        }
		}
		}
		catch (Exception ex) {
			logger.error("Error sending message to topic = "+topic, ex);
			output = false;
		}
		return output;
	}
	
}
