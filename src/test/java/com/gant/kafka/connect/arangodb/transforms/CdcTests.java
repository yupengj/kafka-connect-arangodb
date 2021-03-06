package com.gant.kafka.connect.arangodb.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CdcTests {
	private final Schema keyStructSchema = SchemaBuilder.struct().name("key").version(1).doc("key schema").field("Id", Schema.INT32_SCHEMA).build();
	private final Schema valueStructDocumentSchema = SchemaBuilder.struct().name("valueDocument").version(1).doc("value document schema")
			.field("Name", Schema.STRING_SCHEMA).optional().build();
	private final Schema valueStructSchema = SchemaBuilder.struct().name("value").version(1).doc("value schema").field("before", valueStructDocumentSchema)
			.field("after", valueStructDocumentSchema).optional().build();

	Cdc<SinkRecord> cdcTransformer;

	@BeforeEach
	public void beforeAll() {
		cdcTransformer = new Cdc<>();
	}

	@AfterEach
	public void afterAll() {
		if (cdcTransformer != null) {
			cdcTransformer.close();
		}
	}


	@Test
	public void configureDoesNothing() {
		cdcTransformer.configure(null);
	}

	@Test
	public void applySchemafulAppliesCdcUnwrapTransformations() {
		final Struct keyStub = new Struct(keyStructSchema).put("Id", 1234);
		final Struct valueBeforeStub = new Struct(this.valueStructDocumentSchema).put("Name", "Eleanor");
		final Struct valueAfterStub = new Struct(this.valueStructDocumentSchema).put("Name", "Ellie");
		final Struct valueStub = new Struct(this.valueStructSchema).put("before", valueBeforeStub).put("after", valueAfterStub);

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructSchema, valueStub, 0);

		final SinkRecord expectedRecord = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructDocumentSchema,
				valueAfterStub, 0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertEquals(expectedRecord, transformedRecord);
	}

	@Test
	public void applySchemafulNullValuesAppliesCdcUnwrapTransformations() {
		final Struct keyStub = new Struct(this.keyStructSchema).put("Id", 1234);
		final Struct valueBeforeStub = null;
		final Struct valueAfterStub = null;
		final Struct valueStub = new Struct(this.valueStructSchema).put("before", valueBeforeStub).put("after", valueAfterStub);

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructSchema, valueStub, 0);

		final SinkRecord expectedRecord = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructDocumentSchema, valueAfterStub,
				0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertEquals(expectedRecord, transformedRecord);
	}

	@Test
	public void applySchemafulTombstoneAppliesCdcUnwrapTransformations() {
		final Struct keyStub = new Struct(this.keyStructSchema).put("Id", 1234);
		final Struct valueStub = null;

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, this.keyStructSchema, keyStub, this.valueStructSchema, valueStub, 0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertNull(transformedRecord);
	}

	@Test
	public void applySchemalessAppliesCdcUnwrapTransformations() {
		final Map<String, Object> keyStub = new HashMap<>();
		keyStub.put("Id", 1234);

		final Map<String, Object> valueBeforeStub = new HashMap<>();
		valueBeforeStub.put("Name", "Eleanor");

		final Map<String, Object> valueAfterStub = new HashMap<>();
		valueAfterStub.put("Name", "Ellie");

		final Map<String, Object> valueStub = new HashMap<>();
		valueStub.put("before", valueBeforeStub);
		valueStub.put("after", valueAfterStub);

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, null, keyStub, null, valueStub, 0);

		final SinkRecord expectedRecord = new SinkRecord("ibom.mstdata.md_materila", 1, null, keyStub, null, valueAfterStub, 0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertEquals(expectedRecord, transformedRecord);
	}

	@Test
	public void applySchemalessNullValuesAppliesCdcUnwrapTransformations() {
		final Map<String, Object> keyStub = new HashMap<>();
		keyStub.put("Id", 1234);

		final Map<String, Object> valueBeforeStub = null;

		final Map<String, Object> valueAfterStub = null;

		final Map<String, Object> valueStub = new HashMap<>();
		valueStub.put("before", valueBeforeStub);
		valueStub.put("after", valueAfterStub);

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, null, keyStub, null, valueStub, 0);

		final SinkRecord expectedRecord = new SinkRecord("ibom.mstdata.md_materila", 1, null, keyStub, null, valueAfterStub, 0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertEquals(expectedRecord, transformedRecord);
	}

	@Test
	public void applySchemalessTombstoneAppliesCdcUnwrapTransformations() {
		final Map<String, Object> keyStub = new HashMap<>();
		keyStub.put("Id", 1234);

		final Map<String, Object> valueStub = null;

		final SinkRecord sinkRecordStub = new SinkRecord("ibom.mstdata.md_materila", 1, null, keyStub, null, valueStub, 0);

		SinkRecord transformedRecord = cdcTransformer.apply(sinkRecordStub);

		assertNull(transformedRecord);
	}

	@Test
	public void configReturnsEmptyConfigDef() {
		ConfigDef config = cdcTransformer.config();

		final ConfigDef expectedConfig = new ConfigDef();

		assertEquals(expectedConfig.configKeys(), config.configKeys());
		assertEquals(expectedConfig.defaultValues(), config.defaultValues());
		assertEquals(expectedConfig.names(), config.names());
		assertEquals(expectedConfig.groups(), config.groups());
		assertEquals(expectedConfig.toHtmlTable(), config.toHtmlTable());
	}
}
