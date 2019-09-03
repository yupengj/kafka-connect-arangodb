package com.gant.kafka.connect.arangodb.writer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.gant.kafka.connect.arangodb.ArangoDbSinkConfig;
import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;
import com.gant.kafka.connect.arangodb.util.ArangodbTestUtils;

public class EdgeMetadataConsumerTest {

	private static ArangoDbSinkConfig arangoDbSinkConfig;
	private static EdgeMetadataConsumer edgeMetadataConsumer;
	private static EdgeMetadataCache edgeMetadataCache;

	@BeforeAll
	public static void beforeAll() {
		arangoDbSinkConfig = ArangodbTestUtils.config();
		edgeMetadataCache = new EdgeMetadataCache();
		edgeMetadataConsumer = new EdgeMetadataConsumer(arangoDbSinkConfig, edgeMetadataCache);
	}

	@Test
	public void execute() throws InterruptedException {
		edgeMetadataConsumer.start();

		Thread.sleep(30 * 1000);
		System.out.println("cache size: " + edgeMetadataCache.getCache().size());
		System.out.println("==============");

		for (EdgeMetadata edgeMetadata : edgeMetadataCache.getCache()) {
			System.out.println(edgeMetadata);
		}

		System.out.println("==============");
	}


}