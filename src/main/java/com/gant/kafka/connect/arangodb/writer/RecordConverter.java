package com.gant.kafka.connect.arangodb.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gant.kafka.connect.arangodb.entity.ArangoEdge;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;
import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;


public class RecordConverter {
	private static final Logger LOGGER = LoggerFactory.getLogger(RecordConverter.class);

	// 边关系缓存
	private final EdgeMetadataCache edgeMetadataCache;
	private final JsonConverter jsonConverter;
	private final JsonDeserializer jsonDeserializer;
	private final ObjectMapper objectMapper;

	public RecordConverter(final EdgeMetadataCache edgeMetadataCache, final JsonConverter jsonConverter, final JsonDeserializer jsonDeserializer,
			final ObjectMapper objectMapper) {
		this.edgeMetadataCache = edgeMetadataCache;
		this.jsonConverter = jsonConverter;
		this.jsonDeserializer = jsonDeserializer;
		this.objectMapper = objectMapper;
	}

	public final ArangoVertex convertVertex(final SinkRecord record) {
		return new ArangoVertex(this.getCollection(record), this.getKey(record)[1], this.getValue(record));
	}

	public final List<ArangoEdge> convertEdge(ArangoVertex arangoVertex) {
		if (!isCreateEdge(arangoVertex)) {
			return Collections.emptyList();
		}
		final List<EdgeMetadata> edgeMetadata = getEdgeMetadata(arangoVertex);
		if (edgeMetadata.isEmpty()) {
			return Collections.emptyList();
		}
		final ObjectNode valueNode = convertJsonNode(arangoVertex);
		List<ArangoEdge> arangoEdges = new ArrayList<>();
		for (EdgeMetadata edgeMetadatum : edgeMetadata) {
			final ArangoEdge arangoEdge = createArangoEdge(edgeMetadatum, arangoVertex, valueNode);
			arangoEdges.add(arangoEdge);
		}
		return arangoEdges;
	}


	private String getCollection(final SinkRecord record) {
		final String topic = record.topic();
		return topic.substring(topic.lastIndexOf(".") + 1);
	}


	private String getValue(final SinkRecord record) {
		if (record.value() == null) {
			return null;
		}
		final String[] key = getKey(record);
		final ObjectNode valueJsonObject = getObjectNodeValue(record);

		valueJsonObject.put("_key", key[1]);
		valueJsonObject.remove(key[0]);

		try {
			return this.objectMapper.writeValueAsString(valueJsonObject);
		} catch (JsonProcessingException exception) {
			throw new IllegalArgumentException("record value cannot be serialized to JSON");
		}
	}

	private ObjectNode getObjectNodeValue(final SinkRecord record) {
		if (record.value() == null) {
			return null;
		}

		final byte[] serializedRecord = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
		final JsonNode valueJson = jsonDeserializer.deserialize(record.topic(), serializedRecord);

		if (!valueJson.isObject()) {
			throw new IllegalArgumentException("record value is not a single object/document");
		}

		final ObjectNode valueJsonObject = (ObjectNode) valueJson;
		return valueJsonObject;
	}

	private String[] getKey(final SinkRecord record) {
		if (record.value() == null) {
			return new String[2];
		}
		final String keyFieldName;
		final String keyValue;

		if (record.keySchema() == null) {
			final Map<String, Object> keyStruct = (Map<String, Object>) record.key();
			keyFieldName = keyStruct.keySet().iterator().next();
			keyValue = keyStruct.get(keyFieldName).toString();
		} else {
			final Struct keyStruct = (Struct) record.key();
			keyFieldName = record.keySchema().fields().get(0).name();
			keyValue = keyStruct.get(keyFieldName).toString();
		}
		return new String[]{keyFieldName, keyValue};
	}


	private boolean isCreateEdge(ArangoVertex arangoVertex) {
		if (arangoVertex.collection == null || arangoVertex.collection.length() == 0) {
			return false;
		}
		if (arangoVertex.value == null) { //删除操作, 在 fromCollection 或者在 toCollection 都需要处理
			return edgeMetadataCache.getCache().stream()
					.anyMatch(it -> it.fromCollection.equals(arangoVertex.collection) || it.toCollection.equals(arangoVertex.collection));
		}

		// 新增、修改操作，只处理 fromCollection 中的记录就可以。
		// 1. 连接器处理的都是外键约束
		// 2. 外键约束从源点就可以知道目标点的id，所以知道源点就可以构建边的关系
		// 3. 目标的都是主键，目标点的新增不会产生边，目标点的修改不会修改主键，所以也不会对边产生修改
		return edgeMetadataCache.getCache().stream().anyMatch(it -> it.fromCollection.equals(arangoVertex.collection));
	}

	private List<EdgeMetadata> getEdgeMetadata(ArangoVertex arangoVertex) {
		if (arangoVertex.collection == null || arangoVertex.collection.length() == 0) {
			return Collections.emptyList();
		}
		Predicate<EdgeMetadata> predicate;
		if (arangoVertex.value == null) {
			predicate = it -> it.fromCollection.equals(arangoVertex.collection) || it.toCollection.equals(arangoVertex.collection);
		} else {
			predicate = it -> it.fromCollection.equals(arangoVertex.collection);
		}
		return edgeMetadataCache.getCache().stream().filter(predicate).collect(Collectors.toList());
	}

	private ObjectNode convertJsonNode(ArangoVertex arangoVertex) {
		ObjectNode jsonNodes = null;
		if (arangoVertex.value != null) {
			try {
				jsonNodes = objectMapper.readValue(arangoVertex.value, ObjectNode.class);
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.warn("objectMapper readValue to ObjectNode, value :{},error:{}", arangoVertex.value, e.getMessage());
			}
		}
		return jsonNodes;
	}

	private ArangoEdge createArangoEdge(EdgeMetadata edgeMetadata, final ArangoVertex arangoVertex, final ObjectNode jsonNodes) {
		final String from = arangoVertex.collection + "/" + arangoVertex.key;
		if (jsonNodes == null) {
			return new ArangoEdge(edgeMetadata.edgeCollection, from, null);
		}

		final JsonNode toKey = jsonNodes.get(edgeMetadata.toAttribute);
		if (toKey == null) {
			return new ArangoEdge(edgeMetadata.edgeCollection, from, null);
		}
		String to = edgeMetadata.toCollection + "/" + toKey.toString();
		return new ArangoEdge(edgeMetadata.edgeCollection, from, to);
	}

}
