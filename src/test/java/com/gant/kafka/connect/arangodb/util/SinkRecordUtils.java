package com.gant.kafka.connect.arangodb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordUtils {


	public static List<SinkRecord> create(String topic, List<Map<String, String>> keys, List<Map<String, String>> values) {
		if (keys == null || keys.isEmpty() || values == null || values.isEmpty()) {
			throw new DataException("key or value not null");
		}
		if (keys.size() != values.size()) {
			throw new DataException("key 和 value 长度不相等");
		}

		List<SinkRecord> sinkRecords = new ArrayList<>();
		for (int i = 0; i < keys.size(); i++) {
			sinkRecords.add(create(topic, keys.get(i), values.get(i)));
		}
		return sinkRecords;
	}

	public static SinkRecord create(String topic, Map<String, String> key, Map<String, String> value) {
		if (key == null || key.isEmpty()) {
			throw new DataException("key or value not null");
		}
		Schema keySchema = createSchema(key.keySet());
		Struct keyStruct = createStruct(keySchema, key);

		Schema dataSchema = null;
		Struct dataStruct = null;
		if (value != null) {
			dataSchema = createSchema(value.keySet());
			dataStruct = createStruct(dataSchema, value);
		}

		SinkRecord sinkRecord = new SinkRecord(topic, 0, keySchema, keyStruct, dataSchema, dataStruct, 0);
		return sinkRecord;
	}

	private static Schema createSchema(Set<String> fields) {
		SchemaBuilder builder = SchemaBuilder.struct();
		fields.forEach(it -> builder.field(it, Schema.STRING_SCHEMA));
		return builder.build();
	}

	private static Struct createStruct(Schema schema, Map<String, String> value) {
		Struct struct = new Struct(schema);
		value.forEach(struct::put);
		return struct;
	}
}
