package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gant.kafka.connect.arangodb.entity.ArangoEdge;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;
import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;
import com.gant.kafka.connect.arangodb.util.SinkRecordUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordConverterTests {

	private static RecordConverter recordConverter;
	private static EdgeMetadataCache edgeMetadataCache;

	@BeforeAll
	public static void beforeAll() {
		final JsonConverter jsonConverter = new JsonConverter();
		jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);

		final JsonDeserializer jsonDeserializer = new JsonDeserializer();
		final ObjectMapper objectMapper = new ObjectMapper();

		edgeMetadataCache = new EdgeMetadataCache();
		recordConverter = new RecordConverter(edgeMetadataCache, jsonConverter, jsonDeserializer, objectMapper);
	}

	@Test
	public void convertVertexEquals() {
		Map<String, String> value = new LinkedHashMap<>();
		value.put("materila_num", "1000-05801");
		value.put("materila_name", "喷嘴座");

		final SinkRecord sinkRecord = SinkRecordUtils.create("ibom.mstdata.md_material", Collections.singletonMap("md_material_id", "1234"), value);

		final ArangoVertex arangoVertex = recordConverter.convertVertex(sinkRecord);

		final ArangoVertex expectedArangoVertex = new ArangoVertex("md_material", "1234",
				"{\"materila_num\":\"1000-05801\",\"materila_name\":\"喷嘴座\",\"_key\":\"1234\"}");

		assertEquals(expectedArangoVertex, arangoVertex);
	}

	@Test
	public void convertDeleteVertex() {
		final SinkRecord sinkRecord = SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", "100"), null);
		final ArangoVertex arangoVertex = recordConverter.convertVertex(sinkRecord);

		final ArangoVertex expectedArangoVertex = new ArangoVertex("test1_collection", "100", null);

		assertEquals(expectedArangoVertex, arangoVertex);
	}

	@Test
	public void convertEdgeEquals() {

		edgeMetadataCache.add(new EdgeMetadata("1", "test1_collection", "bm_part_assembly", "master_part_id", "md_material", "md_material_id"));
		edgeMetadataCache.add(new EdgeMetadata("2", "test2_collection", "bm_part_assembly", "sub_part_id", "md_material", "md_material_id"));


		Map<String, String> value = new LinkedHashMap<>();
		value.put("master_part_id", "200");
		value.put("sub_part_id", "300");

		final SinkRecord sinkRecord = SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", "100"), value);
		final ArangoVertex arangoVertex = recordConverter.convertVertex(sinkRecord);

		final List<ArangoEdge> arangoEdges = recordConverter.convertEdge(arangoVertex);

		List<ArangoEdge> expectedArangoEdges = new ArrayList<>();
		expectedArangoEdges.add(new ArangoEdge("test1_collection", "bm_part_assembly/100", "md_material/200"));
		expectedArangoEdges.add(new ArangoEdge("test2_collection", "bm_part_assembly/100", "md_material/300"));

		assertEquals(expectedArangoEdges, arangoEdges);
	}

	@Test
	public void convertDeleteEdge() {

		edgeMetadataCache.add(new EdgeMetadata("1", "test1_collection", "bm_part_assembly", "master_part_id", "md_material", "md_material_id"));
		edgeMetadataCache.add(new EdgeMetadata("2", "test2_collection", "bm_part_assembly", "sub_part_id", "md_material", "md_material_id"));

		final SinkRecord sinkRecord = SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", "100"), null);
		final ArangoVertex arangoVertex = recordConverter.convertVertex(sinkRecord);

		final List<ArangoEdge> arangoEdges = recordConverter.convertEdge(arangoVertex);

		List<ArangoEdge> expectedArangoEdges = new ArrayList<>();
		expectedArangoEdges.add(new ArangoEdge("test1_collection", "bm_part_assembly/100", null));
		expectedArangoEdges.add(new ArangoEdge("test2_collection", "bm_part_assembly/100", null));

		assertEquals(expectedArangoEdges, arangoEdges);
	}
}
