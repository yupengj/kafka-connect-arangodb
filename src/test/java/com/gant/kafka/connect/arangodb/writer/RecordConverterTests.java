package com.gant.kafka.connect.arangodb.writer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;

public class RecordConverterTests {
	private final Schema keyStructSchema = SchemaBuilder.struct().name("key").version(1).doc("key schema").field("md_material_id", Schema.INT32_SCHEMA).build();
	private final Schema valueStructSchema = SchemaBuilder.struct().name("value").version(1).doc("value schema").field("materila_num", Schema.STRING_SCHEMA)
			.field("materila_name", Schema.STRING_SCHEMA).build();

	private static RecordConverter recordConverter;

	@BeforeAll
	public static void beforeAll() {
		final JsonConverter jsonConverter = new JsonConverter();
		jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), false);

		final JsonDeserializer jsonDeserializer = new JsonDeserializer();
		final ObjectMapper objectMapper = new ObjectMapper();

//		recordConverter = new RecordConverter(jsonConverter, jsonDeserializer, objectMapper);
	}

	@Test
	public void convertSchemafulRecordReturnsArangoRecord() {
		// Set up stub data
		final Struct keyStub = new Struct(this.keyStructSchema).put("md_material_id", 1234);
		final Struct valueStub = new Struct(this.valueStructSchema).put("materila_num", "1000-05801").put("materila_name", "喷嘴座");
		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, keyStub.schema(), keyStub, valueStub.schema(), valueStub, 0);

		final ArangoVertex expectedArangoVertex = new ArangoVertex("md_materila", "1234",
				"{\"materila_num\":\"1000-05801\",\"materila_name\":\"喷嘴座\",\"_key\":\"1234\"}");

//		final ArangoVertex arangoVertex = recordConverter.convert(sinkRecordStub);

//		assertEquals(expectedArangoVertex, arangoVertex);
	}

	@Test
	public void convertSchemafulTombstoneRecordReturnsArangoRecord() {
		final Struct keyStub = new Struct(this.keyStructSchema).put("md_material_id", 1234);
		final Struct valueStub = null;
		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructSchema, valueStub, 0);

		final ArangoVertex expectedArangoVertex = new ArangoVertex("md_materila", "1234", null);

//		final ArangoVertex arangoVertex = recordConverter.convert(sinkRecordStub);

//		assertEquals(expectedArangoVertex, arangoVertex);
	}

	@Test
	public void convertSchemafulNonSingleObjectRecordThrowsException() {
		final Struct keyStub = new Struct(this.keyStructSchema).put("md_material_id", 1234);
		final List<Struct> valueStub = Arrays.asList(new Struct(this.valueStructSchema).put("materila_num", "1000-05801").put("materila_name", "喷嘴座"),
				new Struct(this.valueStructSchema).put("materila_num", "1000-05801-1").put("materila_name", "喷嘴座"));

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub,
				SchemaBuilder.array(this.valueStructSchema).build(), valueStub, 0);

//		Exception thrownException = assertThrows(IllegalArgumentException.class, () -> recordConverter.convert(sinkRecordStub));
//		assertEquals("record value is not a single object/document", thrownException.getMessage());
	}
}
