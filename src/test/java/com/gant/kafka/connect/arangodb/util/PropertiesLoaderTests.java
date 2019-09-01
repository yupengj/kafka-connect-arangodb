package com.gant.kafka.connect.arangodb.util;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class PropertiesLoaderTests {

	@Test
	public void constructorDoesNothing() {
		new PropertiesLoader();
	}

	@Test
	public void loadLoadsProperties() {
		final Properties properties = PropertiesLoader.load();

		assertEquals("1.0.0", properties.get("version"));
	}
}
