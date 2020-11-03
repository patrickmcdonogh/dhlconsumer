package dhl.demo;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

public interface KafkaMessageHandler {
	int pollAndHandleMessages();
	void setupKafkaConsumer(String bootstrapServers, String topic, String consumerGroup, boolean autoAck, ConsumerRebalanceListener rebalanceManager);
	void terminatePoll();
}
