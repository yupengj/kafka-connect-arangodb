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
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.gant.kafka.connect.arangodb.ArangoDbSinkConfig;
import com.gant.kafka.connect.arangodb.util.ArangodbTestUtils;

public class ArangoDbJavaDriverTest {

	private static ArangoDbSinkConfig config;
	private static ArangoDatabase database;

	@BeforeAll
	public static void beforeAll() {
		config = ArangodbTestUtils.config();
		database = ArangodbTestUtils.database();
	}

	@Test
	public void findCollection() {
		final Collection<CollectionEntity> collections = database.getCollections();
		System.out.println("size : " + collections.size());
		collections.stream().filter(it -> !it.getIsSystem()).map(CollectionEntity::getName).forEach(System.out::println);
	}


	@Test
	public void createEdge() {
		final ArangoCollection arangoCollection = database.collection("edge_test");
		if (arangoCollection.exists()) {
			arangoCollection.drop();
		}
		arangoCollection.create(new CollectionCreateOptions().type(CollectionType.EDGES));

		final BaseEdgeDocument value = new BaseEdgeDocument();
		value.setFrom("md_material/1");
		value.setTo("md_material/95396666545555122");
		value.setKey("test1_1");
		arangoCollection.insertDocuments(Collections.singleton(value));

	}

	@Test
	public void deleteEdge() {
		final ArangoCollection arangoCollection = database.collection("test_edge");

		final List<String> strings = Arrays.asList("70193", "70209", "70225");

		arangoCollection.deleteDocuments(strings, null, new DocumentDeleteOptions().waitForSync(true).silent(true));

	}

	@Test
	public void updateEdge() {
		final ArangoCollection arangoCollection = database.collection("test_edge");

		BaseEdgeDocument document = new BaseEdgeDocument("72389", "test/111", "test/333");

		arangoCollection.insertDocument(document, new DocumentCreateOptions().overwrite(true).waitForSync(true).silent(true));
	}


	@Test
	public void queryEdge() {
		//["test/111","test/222"]
		String c = "test_edge";
		String queryString = "for i in @v for u in " + c + " filter u._from == i or u._to == i  return distinct remove u._key in " + c;

		Map<String, Object> bindVars = new HashMap<>();
		bindVars.put("v", Arrays.asList("test/111", "test/222"));

		ArangoCursor<String> cursor = database.query(queryString, bindVars, null, String.class);


		System.out.println(cursor.asListRemaining().size());
	}

	@Test
	public void queryDeleteEdge() {

		String collection = "test_edge";
		String aql =
				"for i in @vertexKey for u in test_edge filter u._from == i or u._to == i remove u._key in " + collection + " OPTIONS { ignoreErrors: true, "
						+ "waitForSync: true }";


		database.query(aql, Collections.singletonMap("vertexKey", Arrays.asList("test/111", "test/222")), null, null);
	}

}
