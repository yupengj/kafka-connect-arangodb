package com.gant.kafka.connect.arangodb.util;

import java.util.HashMap;
import java.util.Map;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.gant.kafka.connect.arangodb.ArangoDbSinkConfig;
import com.gant.kafka.connect.arangodb.writer.EdgeMetadataCache;
import com.gant.kafka.connect.arangodb.writer.EdgeMetadataConsumer;

public class ArangodbTestUtils {


	public static ArangoDbSinkConfig config() {
		ArangoDbSinkConfig arangoDbSinkConfig = new ArangoDbSinkConfig(configMap());
		return arangoDbSinkConfig;
	}

	public static Map<String, String> configMap() {
		final Map<String, String> originalsStub = new HashMap<>();
		originalsStub.put("arangodb.host", "192.168.4.109");
		originalsStub.put("arangodb.port", "8529");
		originalsStub.put("arangodb.user", "root");
		originalsStub.put("arangodb.password", "arangodb");
		originalsStub.put("arangodb.database.name", "test_database");

		originalsStub.put("edge.metadata.kafka.servers", "192.168.4.109:9092");
		originalsStub.put("edge.metadata.topic", "ibom.mstdata.md_relation_metadata");

		return originalsStub;
	}


	public static EdgeMetadataCache cache() {
		EdgeMetadataCache edgeMetadataCache = new EdgeMetadataCache();
		EdgeMetadataConsumer edgeMetadataConsumer = new EdgeMetadataConsumer(config(), edgeMetadataCache);
		edgeMetadataConsumer.start();
		try {
			Thread.sleep(15 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return edgeMetadataCache;
	}

	public static ArangoDatabase database() {
		final ArangoDbSinkConfig config = config();
		final ArangoDB arangodb = new ArangoDB.Builder().host(config.arangoDbHost, config.arangoDbPort).user(config.arangoDbUser)
				.password(config.arangoDbPassword.value()).build();

		final ArangoDatabase database = arangodb.db(config.arangoDbDatabaseName);
		if (!database.exists()) {
			database.create();
		}
		return database;
	}

	public static void drop(final ArangoDatabase database) {
		if (database.exists()) {
			database.drop();
		}
	}
}
