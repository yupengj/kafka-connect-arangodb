package com.gant.kafka.connect.arangodb.writer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.gant.kafka.connect.arangodb.ArangoDbSinkConfig;
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
	public void run() throws InterruptedException {
		Thread thread = new Thread(edgeMetadataConsumer);

		thread.start();

		Thread.sleep(5 * 1000);

		edgeMetadataConsumer.shutdown();

		thread.interrupt();

		System.out.println("cache size: " + edgeMetadataCache.getCache().size());
	}


}