package com.poc.kafka.twitter.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	String topic = "twitter_tweets";
	String bootStrapServer = "127.0.0.1:9092";
	String consumerKey = "ERRNcuhnNPVdCZkk7q82J1kob";
	String consumerSecret = "mmZjtfeYtfvehF5sVMzjZNlVRPlxEB6aFQqPQaWcKUZ9slc2bh";
	String token = "949507253261905920-sjUbIYujKDXVjsAEcPIniP9iudiPn5t";
	String secret = "JhLlANBwWQ7XnNJgzSNXLut5nd5xBIqXTBvMS5o0KtPVT";
	List<String> terms = Lists.newArrayList("bitcoin","sport","politics", "cricket", "corona", "COVID");

	public TwitterProducer() {

	}

	public void run() {
		
		logger.info("Start runnung application");

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// Create Twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect();

		// create kafka producer
		KafkaProducer<String, String> producer = createProducer();
		
		//Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down client from twitter");
			client.stop();
			logger.info("closing producer...");
			producer.close(); //Closing the producer means it send all the message into kafka cluster which is started before shutting down. in short it clear the session.
			logger.info("Done!");
		}));

		// loop to send Tweets to kafka.
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {
				logger.info("Message:- {}", msg);
				logger.info("Start sending message to kafka cluser.");
				producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							logger.error("Something bad happened. Exception: {}", exception);
						}
						
					}
				});
			}

		}
		
		logger.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);//follow people
		// which is not required here and optional
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
		// Attempts to establish a connection.

	}
	
	public KafkaProducer<String, String> createProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create a safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//kafka 2.0 is > 1.1 so we can keep as 5 otherwise use 1.
		
		//High throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32kb batch size
		
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		return producer;
	}

}
