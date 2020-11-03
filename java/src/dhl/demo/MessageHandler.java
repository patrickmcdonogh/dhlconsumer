package dhl.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.apama.epl.plugin.Correlator;
import com.apama.util.Logger;

public class MessageHandler implements KafkaMessageHandler {
	final static Logger logger = Logger.getLogger();

	KafkaConsumer<String, String> consumer;

	public MessageHandler() {
	}

	@Override
	public int pollAndHandleMessages() {
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300)); 
		int recordCount = records.count();

		if (recordCount > 0) {
			for (ConsumerRecord<String, String> record : records) {
				try {
					logger.info("Managing message = "+record.value());
					Correlator.sendTo(record.value(), "");
				}
				catch(Exception ex) {
					logger.error("Issue sending data to correlator", ex);
				}
			}
		}
		return recordCount;
	}

	@Override
	public void setupKafkaConsumer(String bootstrapServers, String topic, String consumerGroup, boolean autoAck, ConsumerRebalanceListener rebalanceManager) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		String valueDeserializer = StringDeserializer.class.getName();
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoAck));
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
		properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1280000");

		//switch classloader so that kafka can find the serializers
		ClassLoader original = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(null);

		consumer = new KafkaConsumer<String, String>(properties);

		Thread.currentThread().setContextClassLoader(original);
		consumer.subscribe(Arrays.asList(topic), rebalanceManager);
	}

	@Override
	public void terminatePoll() {
		consumer.unsubscribe();
		consumer.close();
	}

}
