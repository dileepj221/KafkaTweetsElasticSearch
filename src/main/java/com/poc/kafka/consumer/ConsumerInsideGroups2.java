package com.poc.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//Start multiple consumer insatacne and check for re-assignment of partition for every consumer. so when you sent multiple message through producer message will be consumed to the respective assigned partition only.
//How rebalance happened?
//check for AbstractCoordinator and ConsumerCoordinator?
public class ConsumerInsideGroups2 {

	Logger logger = LoggerFactory.getLogger(ConsumerInsideGroups2.class.getName());

	String bootStrapServer = "127.0.0.1:9092";
	String groupId = "my-sixth-application";
	String topic = "first_topic";

	// Create Kafka properties
	public Properties kafkaProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// latest: Always new
																					// message/earliest- from start
																					// message/none -exception thrown
		return properties;
	}
	
	public void consume() {
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties());
		
		//subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		
		//poll for new data
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("Key: {}, Value: {}", record.key(), record.value());
				logger.info("Partition: {}, Offset: {}",record.partition(), record.offset());
			}
		}
		
	}
	
}
