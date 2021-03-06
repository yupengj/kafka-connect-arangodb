package com.gant.kafka.connect.arangodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.gant.kafka.connect.arangodb.util.PropertiesLoader;


public class ArangoDbSinkConnector extends SinkConnector {
	private Map<String, String> config;

	@Override
	public final String version() {
		return PropertiesLoader.load().getProperty("version");
	}

	@Override
	public final void start(final Map<String, String> props) {
		this.config = props;
	}

	@Override
	public final Class<? extends Task> taskClass() {
		return ArangoDbSinkTask.class;
	}

	@Override
	public final List<Map<String, String>> taskConfigs(final int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
		for (int configIndex = 0; configIndex < maxTasks; ++configIndex) {
			taskConfigs.add(this.config);
		}
		return taskConfigs;
	}

	@Override
	public final void stop() {
	}

	@Override
	public final ConfigDef config() {
		return ArangoDbSinkConfig.CONFIG_DEF;
	}
}
