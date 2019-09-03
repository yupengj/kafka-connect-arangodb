package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;
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
		}//from/id from_id
		final ArangoCollection arangoCollection = database.collection(collection);
		if (!arangoCollection.exists()) { // 要删除的数据所在的集合不存在忽略
			return;
		}
		/**
		 * for e in fk_affected_part_change_id_ref_change
		 *     filter [e._from, e._to] any in ["chg_affected_part/7","md_change/223"]
		 *     remove e._key in fk_affected_part_change_id_ref_change
		 */
		String aql = "for i in @vertexKey for u in " + collection + " filter u._from == i or u._to == i remove u._key in " + collection;
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

	@Override
	protected CollectionCreateOptions getCollectionOptions() {
		return new CollectionCreateOptions().waitForSync(true).type(CollectionType.EDGES);
	}
}
