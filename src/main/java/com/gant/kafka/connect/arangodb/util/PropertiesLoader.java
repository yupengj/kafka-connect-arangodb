package com.gant.kafka.connect.arangodb.util;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader {
	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);

	public static Properties load() {
		final Properties properties = new Properties();
		try {
			properties.load(PropertiesLoader.class.getClassLoader().getResourceAsStream("kafka-connect-arangodb.properties"));
		} catch (IOException exception) {
			LOGGER.error("failed to load properties", exception);
		}
		return properties;
	}
}
