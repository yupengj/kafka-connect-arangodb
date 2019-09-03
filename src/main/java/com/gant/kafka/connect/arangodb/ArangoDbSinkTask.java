package com.gant.kafka.connect.arangodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gant.kafka.connect.arangodb.entity.ArangoBase;
import com.gant.kafka.connect.arangodb.entity.ArangoEdge;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;
import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;
import com.gant.kafka.connect.arangodb.util.PropertiesLoader;
import com.gant.kafka.connect.arangodb.writer.EdgeMetadataCache;
import com.gant.kafka.connect.arangodb.writer.EdgeMetadataConsumer;
import com.gant.kafka.connect.arangodb.writer.EdgeWriter;
import com.gant.kafka.connect.arangodb.writer.RecordConverter;
import com.gant.kafka.connect.arangodb.writer.VertexWriter;
import com.gant.kafka.connect.arangodb.writer.Writer;


public class ArangoDbSinkTask extends SinkTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbSinkTask.class);

	private EdgeMetadataCache edgeMetadataCache;
	private EdgeMetadataConsumer edgeMetadataConsumer;
	private RecordConverter recordConverter;
	private Writer vertexWriter;
	private Writer edgeWriter;

	@Override
	public final String version() {
		return PropertiesLoader.load().getProperty("version");
	}

	@Override
	public final void start(final Map<String, String> props) {
		LOGGER.info("ArangoDbSinkTask start. task config: {}", props);

		final ArangoDbSinkConfig config = new ArangoDbSinkConfig(props);
		final ArangoDB arangodb = new ArangoDB.Builder().host(config.arangoDbHost, config.arangoDbPort).user(config.arangoDbUser)
				.password(config.arangoDbPassword.value()).build();

		final ArangoDatabase database = arangodb.db(config.arangoDbDatabaseName);
		if (!database.exists()) {
			database.create();
		}

		final JsonConverter jsonConverter = new JsonConverter();
		jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);

		final JsonDeserializer jsonDeserializer = new JsonDeserializer();

		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);// 转 java 对象时忽略 class 没有的属性

		this.edgeMetadataCache = new EdgeMetadataCache();
		this.edgeMetadataConsumer = new EdgeMetadataConsumer(config, edgeMetadataCache);
		this.edgeMetadataConsumer.start();

		this.recordConverter = new RecordConverter(edgeMetadataCache, jsonConverter, jsonDeserializer, objectMapper);
		this.vertexWriter = new VertexWriter(database);
		this.edgeWriter = new EdgeWriter(database);
	}

	@Override
	public final void put(final Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}
		long start = System.currentTimeMillis();

		final List<ArangoBase> arangoVertices = new ArrayList<>();
		final List<ArangoBase> arangoEdges = new ArrayList<>();
		for (final SinkRecord sinkRecord : records) {
			final ArangoVertex arangoVertex = this.recordConverter.convertVertex(sinkRecord);
			arangoVertices.add(arangoVertex);
			final List<ArangoEdge> arangoEdgeList = this.recordConverter.convertEdge(arangoVertex);
			if (!arangoEdgeList.isEmpty()) {
				arangoEdges.addAll(arangoEdgeList);
			}
		}
		LOGGER.info("convert vertex {} edge {} time {}", arangoVertices.size(), arangoEdges.size(), System.currentTimeMillis() - start);

		start = System.currentTimeMillis();
		this.vertexWriter.write(arangoVertices);
		LOGGER.info("writing {} vertex. time {}", arangoVertices.size(), System.currentTimeMillis() - start);

		start = System.currentTimeMillis();
		arangoEdges.sort(Comparator.comparing(it -> it.collection));// 按 collection 排序，同一个 collection 批量处理
		this.edgeWriter.write(arangoEdges);
		LOGGER.info("writing {} edge. time {}", arangoEdges.size(), System.currentTimeMillis() - start);

		// 删除边关系元数据时，删除边的 collection
		if (!edgeMetadataCache.getRemoveRelation().isEmpty()) {
			LOGGER.info("drop edge collection {}", Arrays.toString(edgeMetadataCache.getRemoveRelation().toArray()));
			edgeWriter.dropCollection(
					edgeMetadataCache.getRemoveRelation().stream().map(EdgeMetadata::getEdgeCollection).distinct().collect(Collectors.toList()));
			edgeMetadataCache.getRemoveRelation().clear();
		}
	}

	@Override
	public final void stop() {
		if (this.edgeMetadataConsumer != null) {
			this.edgeMetadataConsumer.shutdown();
		}
	}
}
