package com.gant.kafka.connect.arangodb.writer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoEdgeCollection;
import com.arangodb.ArangoGraph;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.EdgeEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.gant.kafka.connect.arangodb.config.ArangoDbSinkConfig;

public class ArangoDbJavaDriverTest {

	private static ArangoDbSinkConfig config;
	private static ArangoDB arangodb;

	private static Map<String, Object> buildConfigMap() {
		final Map<String, Object> originalsStub = new HashMap<String, Object>();
		originalsStub.put("arangodb.host", "192.168.1.6");
		originalsStub.put("arangodb.port", "8529");
		originalsStub.put("arangodb.user", "root");
		originalsStub.put("arangodb.password", "arangodb");
		originalsStub.put("arangodb.database.name", "ibom");
		return originalsStub;
	}

	@BeforeAll
	public static void beforeAll() {
		config = new ArangoDbSinkConfig(buildConfigMap());
		arangodb = new ArangoDB.Builder().host(config.arangoDbHost, config.arangoDbPort).user(config.arangoDbUser).password(config.arangoDbPassword.value())
				.build();
	}

	@Test
	public void database() {
		final Collection<String> databases = arangodb.getDatabases();
		System.out.println(Arrays.toString(databases.toArray()));
		assert databases.contains(config.arangoDbDatabaseName);
	}

	@Test
	public void doesNotExistDatabase() {
		final ArangoDatabase db = arangodb.db("test-12");
		final boolean exists = db.exists();
		assert !exists;
	}

	@Test
	public void createDoesNotExistDatabase() {
		final ArangoDatabase db = arangodb.db("test-12");
		if (!db.exists()) {
			final Boolean aBoolean = db.create();
		}

		final ArangoDatabase db1 = arangodb.db("test-12");
		assert db1.exists();
	}

	@Test
	public void dropDatabase() {
		final ArangoDatabase db = arangodb.db("test-12");
		if (db.exists()) {
			db.drop();
		}
		assert !arangodb.getDatabases().contains("test-12");
	}

	@Test
	public void findCollection() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final Collection<CollectionEntity> collections = db.getCollections();
		System.out.println("size : " + collections.size());
		collections.stream().filter(it -> !it.getIsSystem()).map(CollectionEntity::getName).forEach(System.out::println);
	}

	//	@Test
	public void dropCollection() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final Collection<CollectionEntity> collections = db.getCollections();
		collections.stream().filter(it -> !it.getIsSystem()).map(CollectionEntity::getName).forEach(it -> db.collection(it).drop());

	}

	@Test
	public void createDdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);

		final ArangoGraph graph = db.graph("ibom_test");
		if (!graph.exists()) {
			graph.create(Collections.singleton(new EdgeDefinition().collection("edge_test").from("md_material").to("md_material")));
		}

		final BaseEdgeDocument value = new BaseEdgeDocument();
		value.setFrom("md_material/1");
		value.setTo("md_material/3");
		value.setKey("md_material_1_md_material_556699339449949494334");

		final ArangoEdgeCollection edgeCollection = graph.edgeCollection("edge_test");


		final EdgeEntity edge_test = edgeCollection.insertEdge(value);

		final BaseEdgeDocument baseEdgeDocument = graph.edgeCollection("edge_test").getEdge(edge_test.getKey(), BaseEdgeDocument.class);

		System.out.println(baseEdgeDocument);
	}

	@Test
	public void findEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final ArangoGraph graph = db.graph("ibom_test");
		final ArangoEdgeCollection edgeCollection = graph.edgeCollection("edge_test");
		final BaseEdgeDocument baseEdgeDocument = edgeCollection.getEdge("md_material_1_md_material_2", BaseEdgeDocument.class);
		System.out.println(baseEdgeDocument);
	}


	@Test
	public void deleteGraph() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);

		final ArangoGraph graph = db.graph("ibom_test");
		if (!graph.exists()) {
			return;
		}


		graph.drop();
	}

	@Test
	public void createDoc() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		db.collection("test2")
				.insertDocument("{\"name\":\"ddff\",\"_key\":\"pp\"}", new DocumentCreateOptions().overwrite(true).waitForSync(true).silent(true));
	}

	@Test
	public void deleteDoc() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		db.collection("md_material").deleteDocument("2");
	}

	@Test
	public void findDoc() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final BaseDocument md_material = db.collection("md_material").getDocument("2", BaseDocument.class);
		System.out.println(md_material);
	}

	@Test
	public void createEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final ArangoCollection arangoCollection = db.collection("edge_test");

		final BaseEdgeDocument value = new BaseEdgeDocument();
		value.setFrom("md_material/1");
		value.setTo("md_material/95396666545555122");

		arangoCollection.insertDocuments(Collections.singleton(value));
	}

	@Test
	public void createEdgeByJson() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final ArangoCollection arangoCollection = db.collection("test2");

		String s = "{\"_key\":\"tt_123\",\"_from\":\"tt/123\",\"_to\":\"tt/234\"}";

		final DocumentCreateEntity<String> stringDocumentCreateEntity = arangoCollection.insertDocument(s);

		System.out.println(stringDocumentCreateEntity);
	}

	@Test
	public void deleteEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final ArangoCollection arangoCollection = db.collection("test_edge");

		final List<String> strings = Arrays.asList("70193", "70209", "70225");

		arangoCollection.deleteDocuments(strings, null, new DocumentDeleteOptions().waitForSync(true).silent(true));

	}

	@Test
	public void updateEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);
		final ArangoCollection arangoCollection = db.collection("test_edge");

		BaseEdgeDocument document = new BaseEdgeDocument("72389", "test/111", "test/333");

		arangoCollection.insertDocument(document, new DocumentCreateOptions().overwrite(true).waitForSync(true).silent(true));
	}


	@Test
	public void queryEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);


		//["test/111","test/222"]
		String c = "test_edge";
		String queryString = "for i in @v for u in " + c + " filter u._from == i or u._to == i  return distinct remove u._key in " + c;

		Map<String, Object> bindVars = new HashMap<>();
		bindVars.put("v", Arrays.asList("test/111", "test/222"));

		ArangoCursor<String> cursor = db.query(queryString, bindVars, null, String.class);


		System.out.println(cursor.asListRemaining().size());
	}

	@Test
	public void queryDeleteEdge() {
		final ArangoDatabase db = arangodb.db(config.arangoDbDatabaseName);

		String collection = "test_edge";
		String aql =
				"for i in @vertexKey for u in test_edge filter u._from == i or u._to == i remove u._key in " + collection + " OPTIONS { ignoreErrors: true, "
						+ "waitForSync: true }";



		db.query(aql, Collections.singletonMap("vertexKey", Arrays.asList("test/111", "test/222")), null, null);
	}

}
