package com.poc.kafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.poc.kafka.consumer.Consumer1;
import com.poc.kafka.consumer.ConsumerAssignSeek4;
import com.poc.kafka.consumer.ConsumerWithThread3;
import com.poc.kafka.producer.ProducerWithKeys;
import com.poc.kafka.streamapi.StreamsFilterTweets;
import com.poc.kafka.twitter.consumer.ElasticSerchConsumer;
import com.poc.kafka.twitter.producer.TwitterProducer;

@SpringBootApplication
public class KafkaApplicationRunner implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplicationRunner.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// Producer producer = new Producer();
		// ProducerWithCallback producer = new ProducerWithCallback();
		
		/* ProducerWithKeys producer = new ProducerWithKeys(); producer.produce(); */
			
		/* Consumer1 consumer = new Consumer1(); consumer.consume(); */
		
		/*
		 * ConsumerWithThread3 consumer = new ConsumerWithThread3(); consumer.run();
		 */
		
		/*
		 * ConsumerAssignSeek4 as = new ConsumerAssignSeek4(); as.consume();
		 */
		
		
		/* TwitterProducer producer = new TwitterProducer(); producer.run(); */
		 
		
		/*
		 * ElasticSerchConsumer consumer = new ElasticSerchConsumer(); consumer.run();
		 */
		
		StreamsFilterTweets streamsFilterTweets = new StreamsFilterTweets();
		streamsFilterTweets.run();
		
		
	}

}
