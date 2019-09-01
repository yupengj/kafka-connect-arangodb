package com.gant.kafka.connect.arangodb;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;

import com.gant.kafka.connect.arangodb.ArangoDbSinkTask;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArangoDbSinkTaskTests {
	@Test
	public void versionReturnsVersion() {
		final SinkTask task = new ArangoDbSinkTask();
		assertEquals("1.0.4", task.version());
	}

	@Test
	public void stopDoesNothing() {
		final SinkTask task = new ArangoDbSinkTask();
		task.stop();
	}
}
