package com.gant.kafka.connect.arangodb;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoDbSinkConfig extends AbstractConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbSinkConfig.class);

	private static final String ARANGODB_HOST = "arangodb.host";
	private static final String ARANGODB_HOST_DOC = "ArangoDb server host.";
	public final String arangoDbHost;

	private static final String ARANGODB_PORT = "arangodb.port";
	private static final String ARANGODB_PORT_DOC = "ArangoDb server host port number.";
	public final int arangoDbPort;

	private static final String ARANGODB_USER = "arangodb.user";
	private static final String ARANGODB_USER_DOC = "ArangoDb connection username.";
	public final String arangoDbUser;

	private static final String ARANGODB_PASSWORD = "arangodb.password";
	private static final String ARANGODB_PASSWORD_DEFAULT = "";
	private static final String ARANGODB_PASSWORD_DOC = "ArangoDb connection password.";
	public final Password arangoDbPassword;

	private static final String ARANGODB_DATABASE_NAME = "arangodb.database.name";
	private static final String ARANGODB_DATABASE_NAME_DOC = "ArangoDb database name.";
	public final String arangoDbDatabaseName;

	private static final String BOOTSTRAP_SERVERS = "edge.metadata.kafka.servers";
	private static final String BOOTSTRAP_SERVERS_DOC = "读取边关系主题的 kafka 集群地址";
	public final String bootstrapServers;

	private static final String EDGE_METADATA_TOPIC = "edge.metadata.topic";
	private static final String EDGE_METADATA_TOPIC_DOC = "边关系元数据主题名称";
	public final String edgeMetadataTopic;

	private static final String EDGE_METADATA_ATTRIBUTE_MAP = "edge.metadata.attribute.map";
	private static final String EDGE_METADATA_ATTRIBUTE_DOC = "边的元数据在kafka中的属性自动和连接器中对象字段映射";
	private static final String EDGE_METADATA_ATTRIBUTE_DEFAULT = "key:id,edgeCollection:constraint_name,fromCollection:from_table,fromAttribute:from_column,toCollection:to_table,toAttribute:to_column";
	public final String edgeMetadataAttributeMap;

	public static final ConfigDef CONFIG_DEF = new ConfigDef().define(ARANGODB_HOST, Type.STRING, Importance.HIGH, ARANGODB_HOST_DOC)
			.define(ARANGODB_PORT, Type.INT, Importance.HIGH, ARANGODB_PORT_DOC).define(ARANGODB_USER, Type.STRING, Importance.HIGH, ARANGODB_USER_DOC)
			.define(ARANGODB_PASSWORD, Type.PASSWORD, ARANGODB_PASSWORD_DEFAULT, Importance.HIGH, ARANGODB_PASSWORD_DOC)
			.define(ARANGODB_DATABASE_NAME, Type.STRING, Importance.HIGH, ARANGODB_DATABASE_NAME_DOC)
			.define(BOOTSTRAP_SERVERS, Type.STRING, Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
			.define(EDGE_METADATA_TOPIC, Type.STRING, Importance.HIGH, EDGE_METADATA_TOPIC_DOC)
			.define(EDGE_METADATA_ATTRIBUTE_MAP, Type.STRING, EDGE_METADATA_ATTRIBUTE_DEFAULT, Importance.HIGH, EDGE_METADATA_ATTRIBUTE_DOC);

	public ArangoDbSinkConfig(final Map<?, ?> originals) {
		super(CONFIG_DEF, originals, false);

		LOGGER.info("initial config: {}", originals);

		this.arangoDbHost = getString(ARANGODB_HOST);
		this.arangoDbPort = getInt(ARANGODB_PORT);
		this.arangoDbUser = getString(ARANGODB_USER);
		this.arangoDbPassword = getPassword(ARANGODB_PASSWORD);
		this.arangoDbDatabaseName = getString(ARANGODB_DATABASE_NAME);

		this.bootstrapServers = getString(BOOTSTRAP_SERVERS);
		this.edgeMetadataTopic = getString(EDGE_METADATA_TOPIC);
		this.edgeMetadataAttributeMap = getString(EDGE_METADATA_ATTRIBUTE_MAP);
	}
}
