package com.gant.kafka.connect.arangodb.writer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.gant.kafka.connect.arangodb.ArangoDbSinkConfig;
import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;

public class EdgeMetadataConsumer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(EdgeMetadataConsumer.class);

	private final KafkaConsumer<JsonNode, JsonNode> consumer;
	private final ArangoDbSinkConfig config;
	private final EdgeMetadataCache edgeMetadataCache;
	private static Map<String, String> EDGE_METADATA_ATTRIBUTE_MAP;

	public EdgeMetadataConsumer(final ArangoDbSinkConfig config, final EdgeMetadataCache edgeMetadataCache) {
		LOGGER.info("init EdgeMetadataConsumer servers {} topic {}", config.bootstrapServers, config.edgeMetadataTopic);

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_connect_arangodb_edge_metadata");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		consumer = new KafkaConsumer<>(props);
		this.config = config;
		this.edgeMetadataCache = edgeMetadataCache;
		EDGE_METADATA_ATTRIBUTE_MAP = edgeMetadataAttributeMap();
	}


	public void shutdown() {
		if (consumer != null) {
			consumer.wakeup();
		}
	}

	@Override
	public void run() {
		// 启动消费者线程时，清除缓存中数据
		edgeMetadataCache.clear();
		consumer.subscribe(Collections.singletonList(config.edgeMetadataTopic));
		try {
			while (true) {
				ConsumerRecords<JsonNode, JsonNode> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<JsonNode, JsonNode> record : records) {
					edgeMetadataCache.add(convert(record.key(), record.value()));
				}
			}
		} catch (WakeupException e) {
			LOGGER.info(e.getMessage());
		} finally {
			LOGGER.info("close EdgeMetadataConsumer");
			consumer.close();
		}
	}

	private EdgeMetadata convert(JsonNode key, JsonNode value) {
		final JsonNode jsonNodeKey = key.get("payload");
		EdgeMetadata edgeMetadata = new EdgeMetadata();
		edgeMetadata.setKey(getValue(EdgeMetadata.KEY, jsonNodeKey));

		if (value == null || value instanceof NullNode || value.get("payload") == null || value.get("payload") instanceof NullNode
				|| value.get("payload").get("after") == null || value.get("payload").get("after") instanceof NullNode) {
			return edgeMetadata; // 删除元数据操作
		}

		String op = value.get("payload").get("op").asText();
		if ("u".equals(op)) {
			return null; // 不处理修改元数据操作
		}

		final JsonNode jsonNodeValue = value.get("payload").get("after");
		edgeMetadata.setEdgeCollection(getValue(EdgeMetadata.EDGE_COLLECTION, jsonNodeValue));
		edgeMetadata.setFromCollection(getValue(EdgeMetadata.FROM_COLLECTION, jsonNodeValue));
		edgeMetadata.setFromAttribute(getValue(EdgeMetadata.FROM_ATTRIBUTE, jsonNodeValue));
		edgeMetadata.setToCollection(getValue(EdgeMetadata.TO_COLLECTION, jsonNodeValue));
		edgeMetadata.setToAttribute(getValue(EdgeMetadata.TO_ATTRIBUTE, jsonNodeValue));
		return edgeMetadata;
	}

	private String getValue(final String objectFiled, final JsonNode jsonNode) {
		final String objFile = EDGE_METADATA_ATTRIBUTE_MAP.get(objectFiled);
		if (objFile == null || objFile.isEmpty()) {
			throw new ConfigException("边属性映射配置错误，没有找到 " + objFile + " 属性");
		}
		final JsonNode jn = jsonNode.get(objFile);
		if (jn == null) {
			throw new DataException("value " + jsonNode + " 中没有找到" + objFile + " 属性的数据");
		}
		return jn.asText();
	}


	private Map<String, String> edgeMetadataAttributeMap() {
		final String edgeMetadataAttributeMap = config.edgeMetadataAttributeMap;
		if (edgeMetadataAttributeMap == null || edgeMetadataAttributeMap.isEmpty()) {
			throw new ConfigException("没有找到边关系属性映射");
		}
		final String[] maps = edgeMetadataAttributeMap.split(",");
		Map<String, String> map = new HashMap<>();
		for (String str : maps) {
			String[] mapArr = str.split(":");
			map.put(mapArr[0], mapArr[1]);
		}
		return map;
	}

}
