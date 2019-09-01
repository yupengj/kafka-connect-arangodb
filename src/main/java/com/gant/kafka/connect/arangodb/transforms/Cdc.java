package com.gant.kafka.connect.arangodb.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class Cdc<R extends ConnectRecord<R>> implements Transformation<R> {
	private final ConfigDef config = new ConfigDef();

	@Override
	public final void configure(Map<String, ?> configs) {
	}

	@Override
	@SuppressWarnings("unchecked")
	public final R apply(R record) {
		if (record.value() == null) {
			return null;
		}

		final Schema valueSchema;
		final Object value;

		if (record.valueSchema() == null) {
			// Schemaless
			final Map<String, Object> preTransformValue = (Map<String, Object>) record.value();
			valueSchema = null;
			value = preTransformValue.get("after");
		} else {
			// Schemaful
			final Struct preTransformValue = (Struct) record.value();
			valueSchema = record.valueSchema().field("after").schema();
			value = preTransformValue.get("after");
		}

		return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), valueSchema, value, record.timestamp());
	}

	@Override
	public final ConfigDef config() {
		return this.config;
	}

	@Override
	public final void close() {
	}
}
