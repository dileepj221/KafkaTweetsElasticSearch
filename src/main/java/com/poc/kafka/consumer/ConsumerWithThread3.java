package com.poc.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithThread3 {

	Logger logger = LoggerFactory.getLogger(ConsumerWithThread3.class.getName());

	public ConsumerWithThread3() {

	}

	public void run() {
		String bootStrapServer = "127.0.0.1:9092";
		String groupId = "my-seventh-application";
		String topic = "first_topic";

		// Latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);

		// Create the consumer runnable
		logger.info("Creating the consumer thread");
		Runnable consumerRunnable = new ConsumerRunnable(bootStrapServer, groupId, topic, latch);

		// Start the thread
		Thread thread = new Thread(consumerRunnable);
		thread.start();

		// Add the shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Cought shutdown hook");
			((ConsumerRunnable) consumerRunnable).shutDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application has interrupted \n exception: {}", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerRunnable implements Runnable {

		KafkaConsumer<String, String> consumer;
		private CountDownLatch latch;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

		public ConsumerRunnable(String bootStrapServer, String groupId, String topic, CountDownLatch latch) {
			this.latch = latch;

			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// latest: Always new
																						// message/earliest- from start
																						// message/none -exception
																						// thrown

			// create consumer
			consumer = new KafkaConsumer<>(properties);

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
		}

		@Override
		public void run() {
			consume();
		}

		public void shutDown() {
			// the wakeup method is special method to interrupt consumer.poll()
			// It will throw the exception WakeUpException
			consumer.wakeup();
		}

		public void consume() {
			try {

				// poll for new data
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						logger.info("Key: {}, Value: {}", record.key(), record.value());
						logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Recieved shutdown signal");
			} finally {
				consumer.close();
				// Tell you main code that we are done with consumer.
				latch.countDown();
			}
		}

	}

}
