package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.gant.kafka.connect.arangodb.entity.ArangoBase;
import com.gant.kafka.connect.arangodb.entity.ArangoEdge;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;
import com.gant.kafka.connect.arangodb.util.ArangodbTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class WriterTests {

	private static Writer vertexWriter;
	private static Writer edgeWriter;

	private static ArangoDatabase arangoDatabase;

	@BeforeEach
	public void beforeAll() {
		arangoDatabase = ArangodbTestUtils.database();
		vertexWriter = new VertexWriter(arangoDatabase);
		edgeWriter = new EdgeWriter(arangoDatabase);

	}

	@AfterEach
	public void afterAll() {
		ArangodbTestUtils.drop(arangoDatabase);
	}

	@Test
	public void createVertex() {
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aa\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_2", "1", "{\"a\":\"bb\",\"_key\":\"1\"}"));

		vertexWriter.write(arangoBases);

		final BaseDocument test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		final BaseDocument test2 = arangoDatabase.collection("test_2").getDocument("1", BaseDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("aa", test1.getProperties().get("a"));
		assertEquals("bb", test2.getProperties().get("a"));
	}

	@Test
	public void updateVertex() {
		// 新增
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aa\",\"b\":\"cc\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_2", "1", "{\"a\":\"bb\",\"b\":\"dd\",\"_key\":\"1\"}"));

		vertexWriter.write(arangoBases);

		BaseDocument test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		BaseDocument test2 = arangoDatabase.collection("test_2").getDocument("1", BaseDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("aa", test1.getProperties().get("a"));
		assertEquals("bb", test2.getProperties().get("a"));
		assertEquals("cc", test1.getProperties().get("b"));
		assertEquals("dd", test2.getProperties().get("b"));

		// 修改

		arangoBases.clear();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aaa\",\"b\":\"cc\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_2", "1", "{\"a\":\"bbb\",\"b\":\"dd\",\"_key\":\"1\"}"));

		vertexWriter.write(arangoBases);

		test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		test2 = arangoDatabase.collection("test_2").getDocument("1", BaseDocument.class);

		assertEquals("aaa", test1.getProperties().get("a"));
		assertEquals("bbb", test2.getProperties().get("a"));
		assertEquals("cc", test1.getProperties().get("b"));
		assertEquals("dd", test2.getProperties().get("b"));
	}

	@Test
	public void deleteVertex() {
		// 新增
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aa\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_2", "1", "{\"a\":\"bb\",\"_key\":\"1\"}"));

		vertexWriter.write(arangoBases);

		BaseDocument test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		BaseDocument test2 = arangoDatabase.collection("test_2").getDocument("1", BaseDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("aa", test1.getProperties().get("a"));
		assertEquals("bb", test2.getProperties().get("a"));

		// 删除

		arangoBases.clear();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aa\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_2", "1", null));

		vertexWriter.write(arangoBases);

		test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		test2 = arangoDatabase.collection("test_2").getDocument("1", BaseDocument.class);

		assertNotNull(test1);
		assertNull(test2);

		assertEquals("aa", test1.getProperties().get("a"));
	}

	@Test
	public void saveVertex() {
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aa\",\"b\":\"cc\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_1", "2", "{\"a\":\"bb\",\"b\":\"dd\",\"_key\":\"2\"}"));
		arangoBases.add(new ArangoVertex("test_1", "1", "{\"a\":\"aaa\",\"b\":\"ccc\",\"_key\":\"1\"}"));
		arangoBases.add(new ArangoVertex("test_1", "1", null));

		vertexWriter.write(arangoBases);

		BaseDocument test1 = arangoDatabase.collection("test_1").getDocument("1", BaseDocument.class);
		BaseDocument test2 = arangoDatabase.collection("test_1").getDocument("2", BaseDocument.class);

		assertNull(test1);
		assertNotNull(test2);

		assertEquals("bb", test2.getProperties().get("a"));
		assertEquals("dd", test2.getProperties().get("b"));
	}

	@Test
	public void createEdge() {
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoEdge("test_1", "test1/1", "test2/1"));
		arangoBases.add(new ArangoEdge("test_2", "test3/1", "test4/1"));

		edgeWriter.write(arangoBases);

		BaseEdgeDocument test1 = arangoDatabase.collection("test_1").getDocument("test1_1", BaseEdgeDocument.class);
		BaseEdgeDocument test2 = arangoDatabase.collection("test_2").getDocument("test3_1", BaseEdgeDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("test1/1", test1.getFrom());
		assertEquals("test2/1", test1.getTo());

		assertEquals("test3/1", test2.getFrom());
		assertEquals("test4/1", test2.getTo());
	}

	@Test
	public void updateEdge() {
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoEdge("test_1", "test1/1", "test2/1"));
		arangoBases.add(new ArangoEdge("test_2", "test3/1", "test4/1"));

		edgeWriter.write(arangoBases);

		BaseEdgeDocument test1 = arangoDatabase.collection("test_1").getDocument("test1_1", BaseEdgeDocument.class);
		BaseEdgeDocument test2 = arangoDatabase.collection("test_2").getDocument("test3_1", BaseEdgeDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("test1/1", test1.getFrom());
		assertEquals("test2/1", test1.getTo());

		assertEquals("test3/1", test2.getFrom());
		assertEquals("test4/1", test2.getTo());

		// 修改
		arangoBases.clear();
		arangoBases.add(new ArangoEdge("test_1", "test1/1", "test2/2"));
		arangoBases.add(new ArangoEdge("test_2", "test3/1", "test4/2"));

		edgeWriter.write(arangoBases);

		test1 = arangoDatabase.collection("test_1").getDocument("test1_1", BaseEdgeDocument.class);
		test2 = arangoDatabase.collection("test_2").getDocument("test3_1", BaseEdgeDocument.class);

		assertNotNull(test1);
		assertNotNull(test2);

		assertEquals("test1/1", test1.getFrom());
		assertEquals("test2/2", test1.getTo());

		assertEquals("test3/1", test2.getFrom());
		assertEquals("test4/2", test2.getTo());
	}

	@Test
	public void deleteEdge() {
		List<ArangoBase> arangoBases = new ArrayList<>();
		arangoBases.add(new ArangoEdge("test_1", "test1/1", "test2/1"));
		arangoBases.add(new ArangoEdge("test_2", "test3/1", "test4/1"));

		edgeWriter.write(arangoBases);

		// 删除

		arangoBases.clear();
		arangoBases.add(new ArangoEdge("test_1", "test1/1", "test2/2"));
		arangoBases.add(new ArangoEdge("test_2", "test3/1", null));

		edgeWriter.write(arangoBases);

		BaseEdgeDocument test1 = arangoDatabase.collection("test_1").getDocument("test1_1", BaseEdgeDocument.class);
		BaseEdgeDocument test2 = arangoDatabase.collection("test_2").getDocument("test3_1", BaseEdgeDocument.class);

		assertNotNull(test1);
		assertNull(test2);

		assertEquals("test1/1", test1.getFrom());
		assertEquals("test2/2", test1.getTo());
	}
}
