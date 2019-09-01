package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.model.DocumentCreateOptions;
import com.gant.kafka.connect.arangodb.entity.ArangoBase;
import com.gant.kafka.connect.arangodb.entity.ArangoEdge;

public class EdgeWriter extends Writer {

	public EdgeWriter(ArangoDatabase database) {
		super(database);
	}

	@Override
	protected void deleteBatch(String collection, List<ArangoBase> records) {
		if (collection == null || collection.isEmpty() || records == null || records.isEmpty()) {
			return;
		}
		String aql = "for i in @vertexKey for u in test_edge filter u._from == i or u._to == i remove u._key in " + collection;
		String options = " options { ignoreErrors: true, waitForSync: true }"; // 忽略 doc 不存在时报错，同步等待

		List<String> vertexs = records.stream().map(it -> ((ArangoEdge) it).getFrom()).collect(Collectors.toList());
		database.query(aql + options, Collections.singletonMap("vertexKey", vertexs), null, null);
	}

	@Override
	protected void repsertBatch(String collection, List<ArangoBase> records) {
		final List<BaseEdgeDocument> documentValues = new ArrayList<>();
		for (ArangoBase record : records) {
			ArangoEdge arangoEdge = (ArangoEdge) record;
			documentValues.add(arangoEdge.baseEdgeDocument);
		}
		getCollection(collection).insertDocuments(documentValues, new DocumentCreateOptions().overwrite(true).waitForSync(true).silent(true));
	}
}
