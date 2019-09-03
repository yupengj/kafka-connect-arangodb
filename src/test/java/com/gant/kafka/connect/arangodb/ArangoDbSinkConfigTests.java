package com.gant.kafka.connect.arangodb;


import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class ArangoDbSinkConfigTests {

	private Map<String, Object> buildConfigMap() {
		final Map<String, Object> originalsStub = new HashMap<String, Object>();
		originalsStub.put("arangodb.host", "192.168.4.109");
		originalsStub.put("arangodb.port", "8529");
		originalsStub.put("arangodb.user", "root");
		originalsStub.put("arangodb.password", "arangodb");
		originalsStub.put("arangodb.database.name", "ibom");

		originalsStub.put("edge.metadata.kafka.servers", "192.168.4.109:9092");
		originalsStub.put("edge.metadata.topic", "ibom.mstdata.md_relation_metadata");
		return originalsStub;
	}

	@Test
	public void configTest() {
		ArangoDbSinkConfig arangoDbSinkConfig = new ArangoDbSinkConfig(buildConfigMap());

		assert arangoDbSinkConfig.arangoDbHost.equals("192.168.4.109");
		assert arangoDbSinkConfig.arangoDbPort == 8529;
		assert arangoDbSinkConfig.arangoDbUser.equals("root");
		assert arangoDbSinkConfig.arangoDbPassword.value().equals("arangodb");
		assert arangoDbSinkConfig.arangoDbDatabaseName.equals("ibom");

		assert arangoDbSinkConfig.bootstrapServers.equals("192.168.4.109:9092");
		assert arangoDbSinkConfig.edgeMetadataTopic.equals("ibom.mstdata.md_relation_metadata");
		assert arangoDbSinkConfig.edgeMetadataAttributeMap
				.equals("key:id,edgeCollection:constraint_name,fromCollection:from_table,fromAttribute:from_column,toCollection:to_table,toAttribute:to_column");
	}
}
