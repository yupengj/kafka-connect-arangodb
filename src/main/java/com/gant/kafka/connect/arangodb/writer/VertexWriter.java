package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDatabase;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.gant.kafka.connect.arangodb.entity.ArangoBase;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;


public class VertexWriter extends Writer {
	private static final Logger LOGGER = LoggerFactory.getLogger(VertexWriter.class);

	public VertexWriter(ArangoDatabase database) {
		super(database);
	}

	protected void deleteBatch(final String collection, final List<ArangoBase> records) {
		final List<String> documentKeys = new ArrayList<>();
		for (ArangoBase record : records) {
			documentKeys.add(record.key);
		}
		getCollection(collection).deleteDocuments(documentKeys, null, new DocumentDeleteOptions().silent(true));
	}

	protected void repsertBatch(final String collection, final List<ArangoBase> records) {
		final List<String> documentValues = new ArrayList<>();

		for (ArangoBase record : records) {
			ArangoVertex arangoVertex = (ArangoVertex) record;
			documentValues.add(arangoVertex.value);
		}
		getCollection(collection).insertDocuments(documentValues, new DocumentCreateOptions().overwrite(true).silent(true));
	}

	@Override
	protected CollectionCreateOptions getCollectionOptions() {
		return new CollectionCreateOptions().waitForSync(true);
	}
}
