package com.poc.kafka.streamapi;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {
	
	String bootStrapServer = "127.0.0.1:9092";
	String applicationIdConfig = "demo-kafka-stream";
	String topic = "first_topic";
	
	//Create a Properties
	public Properties kafkaProperties() {
		Properties properties =  new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdConfig);
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		return properties;
	}
	
	
	//Create a topology
	
	public KafkaStreams filterMessage() {
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// create a input topic
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
		KStream<String, String> filteredStream = inputTopic
				.filter((k, jsonValue) -> extractIdFromTweet(jsonValue) > 10000);
		filteredStream.to("filtered_tweets");
		
		//build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaProperties());
		return kafkaStreams;
	}
	
	
	
	//Start our stream application
	public void run() {
		KafkaStreams streams = filterMessage();
		streams.start();
	}
	
	
	private Integer extractIdFromTweet(String value) {
		JsonParser jsonParser = new JsonParser();
		try {
			return jsonParser.parse(value).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
					.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}

}
