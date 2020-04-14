package com.poc.kafka.twitter.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSerchConsumer {
	
	Logger logger = LoggerFactory.getLogger(ElasticSerchConsumer.class.getName());
	
	/*
	 * https://ktoeol7zbo:7ahwcmnals@kafka-tweets-1789578076.ap-southeast-2.
	 * bonsaisearch.net:443
	 */	
	/* get these details from bonsai access tab -> url */
	String hostname = "kafka-tweets-1789578076.ap-southeast-2.bonsaisearch.net";
	String userName = "ktoeol7zbo";
	String password ="7ahwcmnals";
	
	String bootStrapServer = "127.0.0.1:9092";
	String groupId = "my-elasticsearch-apps";
	String topic = "twitter_tweets";
	
	public ElasticSerchConsumer() {
		
	}
	
	public void run() {
		try {
			// Create client
			RestHighLevelClient client = createClient();

			KafkaConsumer<String, String> consumer = createConsumer(topic);

			// poll for new data
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				
				BulkRequest bulkRequest = new BulkRequest();
				Integer recordCount = records.count();

				logger.info("Total " + recordCount +" Records");
					for (ConsumerRecord<String, String> record : records) {
						// here we insert data into ElasticSearch

						// there are 2 strategy to generate id
						// Kafka generic ID 1
						// String id = record.topic() + "_" + record.partition() + "_" +
						// record.offset();

						try {
							// get id from twitter
							String id = extractIdFromTweet(record.value());

							// put the data into elasticsearch
							IndexRequest request = new IndexRequest("twitter", "tweets", id // this is to make our consumer
																							// IDEMPOTENT
							).source(record.value(), XContentType.JSON);

							bulkRequest.add(request);
						}catch (NullPointerException e) {
							logger.warn("Skip bad data: {}",record.value());
						}
						

						// Use below code when you sent every index once in ElasticSearch or use
						// bulkRequest to send index on bulk.
						/*
						 * IndexResponse response = null;
						 * 
						 * response = client.index(request, RequestOptions.DEFAULT);
						 * 
						 * logger.info("Id: {}", response.getId()); Thread.sleep(1000);
						 */
					}
				if (recordCount > 0) {
					BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

					logger.info("Commiting offset...");
					consumer.commitAsync();
					logger.info("Offset has been commited.");
					Thread.sleep(1000);
				}
			}

			// close the client gracefully
			// client.close();
		} catch (IOException e) {
			logger.error("Something bad happen! Exception: {}", e);
		} catch (InterruptedException e) {
			logger.error("Something bad happen! Exception: {}", e);
		}
	}
	
	

	private String extractIdFromTweet(String value) {
		JsonParser jsonParser = new JsonParser();
		return jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
	}

	// Create Kafka Consumer
	public KafkaConsumer<String, String> createConsumer(String topic) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// latest: Always new
																					// message/earliest- from start
																					// message/none -exception thrown
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable offset auto commit
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		//subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	//create client
	public RestHighLevelClient createClient() {
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new HttpClientConfigCallback() {
			
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
		
	}

}
