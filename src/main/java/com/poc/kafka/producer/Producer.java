package com.poc.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
	
	String bootStrapServer = "127.0.0.1:9092";
	
	//Create Kafka properties
	public Properties kafkaProperties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}
	
	
	
	
	public void produce() {
		//Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties());
		
		//create a Produce Record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "This message is sent by java application");
		
		//send data - Async
		producer.send(record);
		
		//flush producer
		producer.flush();
		
		//flush and close producer
		producer.close();
	}
	
	

}
