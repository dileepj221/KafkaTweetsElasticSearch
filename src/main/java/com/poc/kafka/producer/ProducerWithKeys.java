package com.poc.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

	Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

	String bootStrapServer = "127.0.0.1:9092";

	// Create Kafka properties
	public Properties kafkaProperties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public void produce() throws InterruptedException, ExecutionException {

		// Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties());

		for (int i = 1; i <= 10; i++) {
			
			String topic = "first_topic";
			String value = "This message is sent by java application with callback function" + Integer.toString(i);
			String key = "id_"+Integer.toString(i);
			// create a Produce Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,
					value);
			
			logger.info("Key: "+key);
			//ID_1 is going to Partition Partition 0
			//ID_2 Partition 2
			//ID_3 Partition 0
			//ID_4 Partition 2
			//ID_5 Partition 2
			//ID_6 Partition 0
			//ID_7 Partition 2
			//ID_8 Partition 1
			//ID_9 Partition 2
			//ID_10 Partition 2
			
			//now for every run partition is fixed for these key.
			
			
			// send data - Async
			producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					// it executes every time a record is successfully sent or an exception is
					// thrown.
					if (null == e) {
						// the record was successfully sent
						logger.info("Recieved new matadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partitions: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp()*24/1000);

					} else {
						logger.error("Error while producing: {}", e);
					}

				}
			}).get(); //block the .send() to make it  synchronous - but it will reduce the performance.
			//by adding the key, once message is placed based on the in the partition then next time it will produce to same partition for each key.
			
			
			
			
		}
		// flush producer
		producer.flush();

		// flush and close producer
		producer.close();
	}

}
