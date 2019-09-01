package com.gant.kafka.connect.arangodb.writer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.util.ShutdownableThread;

import com.fasterxml.jackson.databind.JsonNode;
import com.gant.kafka.connect.arangodb.config.ArangoDbSinkConfig;

public class EdgeMetadataConsumer extends ShutdownableThread {

	private final KafkaConsumer<JsonNode, JsonNode> consumer;
	private final String topic;
	private final EdgeMetadataCache edgeMetadataCache;

	public EdgeMetadataConsumer(final ArangoDbSinkConfig config, final EdgeMetadataCache edgeMetadataCache) {
		super("edgeMetadataConsumer", false);

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_connect_arangodb_edge_metadata");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		consumer = new KafkaConsumer<>(props);
		this.topic = null;
		this.edgeMetadataCache = edgeMetadataCache;
	}


	@Override
	public void execute() {
		consumer.subscribe(Collections.singletonList(this.topic));
		ConsumerRecords<JsonNode, JsonNode> records = consumer.poll(Duration.ofSeconds(1));
		for (ConsumerRecord<JsonNode, JsonNode> record : records) {
			// relationCache

			record.key();
		}
	}


}
