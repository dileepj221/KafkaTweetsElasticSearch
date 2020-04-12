package com.poc.kafka.consumer;

import java.lang.annotation.Annotation;
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
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

public class ConsumerAssignSeek4 {

	Logger logger = LoggerFactory.getLogger(ConsumerAssignSeek4.class.getName());

	String bootStrapServer = "127.0.0.1:9092";
	String topic = "first_topic";

	// Create Kafka properties
	public Properties kafkaProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// latest: Always new
																					// message/earliest- from start
																					// message/none -exception thrown
		return properties;
	}
	
	public void consume() {
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties());
		
		//Assign and seek are mostly used to reply data or fetch a specific message
		//assign
		org.apache.kafka.common.TopicPartition partitionToReadFrom = new org.apache.kafka.common.TopicPartition(topic, 0);
		long offsetToReadFrom = 15l;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		
		int numberOfMessageToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessageReadSoFar = 0;
		
		//poll for new data
		while(keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				numberOfMessageReadSoFar += 1;
				logger.info("Key: {}, Value: {}", record.key(), record.value());
				logger.info("Partition: {}, Offset: {}",record.partition(), record.offset());
				if(numberOfMessageReadSoFar >= numberOfMessageToRead) {
					keepOnReading = false; //To Exit the while loop
					break; //To exit the for loop
				}
			}
		}
		
	}
	
}
