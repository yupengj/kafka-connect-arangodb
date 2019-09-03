package com.gant.kafka.connect.arangodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.MultiDocumentEntity;
import com.gant.kafka.connect.arangodb.util.ArangodbTestUtils;
import com.gant.kafka.connect.arangodb.util.SinkRecordUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ArangoDbSinkTaskTests {

	private SinkTask task;
	private ArangoDatabase arangoDatabase;

	@BeforeEach
	public void start() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
		task = new ArangoDbSinkTask();
		task.start(ArangodbTestUtils.configMap());
		arangoDatabase = ArangodbTestUtils.database();
		Thread.sleep(5 * 1000);
	}

	@Test
	public void put() {
		List<SinkRecord> sinkRecords = new ArrayList<>();

		List<String> key1 = new ArrayList<>();
		List<String> edgeKey1 = new ArrayList<>();

		int count = 10;
		for (int i = 0; i < count; i++) {
			Map<String, String> value = new HashMap<>();
			value.put("bm_part_assembly_id", i + "");
			value.put("quantity", i + "");
			value.put("master_part_id", i + "");
			value.put("sub_part_id", (count - i) + "");
			value.put("test1", "test1");

			key1.add(i + "");

			edgeKey1.add("bm_part_assembly_" + i);

			sinkRecords.add(SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", i + ""), value));
		}

		for (int i = 0; i < count; i++) {
			Map<String, String> value = new HashMap<>();
			value.put("md_material_id", i + "");
			value.put("material_num", "PPP" + i);

			sinkRecords.add(SinkRecordUtils.create("ibom.mstdata.md_material", Collections.singletonMap("md_material_id", i + ""), value));
		}

		// 新增
		task.put(sinkRecords);

		final MultiDocumentEntity<BaseDocument> baseDocument1 = arangoDatabase.collection("bm_part_assembly").getDocuments(key1, BaseDocument.class);
		assertEquals(10, baseDocument1.getDocuments().size());

		final MultiDocumentEntity<BaseDocument> baseDocument2 = arangoDatabase.collection("md_material").getDocuments(key1, BaseDocument.class);
		assertEquals(10, baseDocument2.getDocuments().size());

		String edge1 = "fk_part_assembly_master_part_id_ref_material";
		String edge2 = "fk_part_assembly_sub_part_id_ref_material";

		final MultiDocumentEntity<BaseEdgeDocument> baseEdgeDocument1 = arangoDatabase.collection(edge1).getDocuments(edgeKey1, BaseEdgeDocument.class);
		assertEquals(10, baseEdgeDocument1.getDocuments().size());
		System.out.println("EdgeDocument collection " + edge1 + " add data: ======");
		baseEdgeDocument1.getDocuments().forEach(System.out::println);

		final MultiDocumentEntity<BaseEdgeDocument> baseEdgeDocument2 = arangoDatabase.collection(edge2).getDocuments(edgeKey1, BaseEdgeDocument.class);
		assertEquals(10, baseEdgeDocument2.getDocuments().size());
		System.out.println("EdgeDocument collection " + edge2 + " add data: ======");
		baseEdgeDocument2.getDocuments().forEach(System.out::println);

		BaseDocument baseDocument = arangoDatabase.collection("bm_part_assembly").getDocument("4", BaseDocument.class);
		assertEquals("4", baseDocument.getProperties().get("quantity"));
		assertNull(baseDocument.getProperties().get("bm_part_assembly_id"));
		assertEquals("4", baseDocument.getProperties().get("master_part_id"));
		assertEquals("6", baseDocument.getProperties().get("sub_part_id"));
		assertEquals("test1", baseDocument.getProperties().get("test1"));

		BaseEdgeDocument baseEdgeDocument = arangoDatabase.collection(edge2).getDocument("bm_part_assembly_4", BaseEdgeDocument.class);
		assertEquals("bm_part_assembly/4", baseEdgeDocument.getFrom());
		assertEquals("md_material/6", baseEdgeDocument.getTo());

		// 修改

		Map<String, String> value = new HashMap<>();
		value.put("bm_part_assembly_id", "4");
		value.put("quantity", "8");// 4->8
		value.put("master_part_id", "4");
		value.put("sub_part_id", "9");// 6 ->9
		value.put("test2", "test2");// add
//		value.put("test1", "test1");// remove

		sinkRecords.clear();
		sinkRecords.add(SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", "4"), value));

		task.put(sinkRecords);

		baseDocument = arangoDatabase.collection("bm_part_assembly").getDocument("4", BaseDocument.class);
		assertEquals("8", baseDocument.getProperties().get("quantity"));
		assertNull(baseDocument.getProperties().get("bm_part_assembly_id"));
		assertEquals("4", baseDocument.getProperties().get("master_part_id"));
		assertEquals("9", baseDocument.getProperties().get("sub_part_id"));
		assertNull(baseDocument.getProperties().get("test1"));
		assertEquals("test2", baseDocument.getProperties().get("test2"));

		baseEdgeDocument = arangoDatabase.collection(edge2).getDocument("bm_part_assembly_4", BaseEdgeDocument.class);
		assertEquals("bm_part_assembly/4", baseEdgeDocument.getFrom());
		assertEquals("md_material/9", baseEdgeDocument.getTo());

		baseEdgeDocument = arangoDatabase.collection(edge1).getDocument("bm_part_assembly_4", BaseEdgeDocument.class);
		assertEquals("bm_part_assembly/4", baseEdgeDocument.getFrom());
		assertEquals("md_material/4", baseEdgeDocument.getTo());

		baseEdgeDocument = arangoDatabase.collection(edge1).getDocument("bm_part_assembly_2", BaseEdgeDocument.class);
		assertEquals("bm_part_assembly/2", baseEdgeDocument.getFrom());
		assertEquals("md_material/2", baseEdgeDocument.getTo());

		// 删除

		sinkRecords.clear();
		// bom key = 4 的顶点，自动删除两个 from 是 bm_part_assembly_4 的边数据
		sinkRecords.add(SinkRecordUtils.create("ibom.bommgmt.bm_part_assembly", Collections.singletonMap("bm_part_assembly_id", "4"), null));

		// 删除 material key = 2 的顶点，自动删除 to 是 md_material/2 的边数据
		sinkRecords.add(SinkRecordUtils.create("ibom.mstdata.md_material", Collections.singletonMap("md_material_id", "2"), null));

		task.put(sinkRecords);

		baseEdgeDocument = arangoDatabase.collection(edge2).getDocument("bm_part_assembly_4", BaseEdgeDocument.class);
		assertNull(baseEdgeDocument);

		baseEdgeDocument = arangoDatabase.collection(edge1).getDocument("bm_part_assembly_4", BaseEdgeDocument.class);
		assertNull(baseEdgeDocument);

		baseEdgeDocument = arangoDatabase.collection(edge1).getDocument("bm_part_assembly_2", BaseEdgeDocument.class);
		assertNull(baseEdgeDocument);
	}


	@AfterEach
	public void stopDoesNothing() {
		task = new ArangoDbSinkTask();
		task.stop();
		ArangodbTestUtils.drop(arangoDatabase);
	}
}
