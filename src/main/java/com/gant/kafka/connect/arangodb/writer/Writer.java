package com.gant.kafka.connect.arangodb.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.CollectionCreateOptions;
import com.gant.kafka.connect.arangodb.entity.ArangoBase;


public abstract class Writer {
	private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

	protected final ArangoDatabase database;

	public Writer(final ArangoDatabase database) {
		this.database = database;

	}

	public final void dropCollection(final Collection<String> collections) {
		if (collections == null || collections.isEmpty()) {
			return;
		}
		for (String collection : collections) {
			final ArangoCollection collection1 = database.collection(collection);
			if (collection1.exists()) {
				collection1.drop();
			}
		}
	}


	public final void write(final Collection<ArangoBase> records) {
		if (records.isEmpty()) {
			return;
		}
		List<ArangoBase> batch = null;
		String batchCollection = null;
		String batchOperation = null;

		final Iterator<ArangoBase> recordIterator = records.iterator();

		while (recordIterator.hasNext()) {
			final ArangoBase record = recordIterator.next();
			final String recordCollection = record.collection;
			final String recordOperation = record.op;

			if (batch == null) {
				batch = new ArrayList<>();
				batchCollection = recordCollection;
				batchOperation = recordOperation;
			}

			if (recordCollection.equals(batchCollection) && recordOperation.equals(batchOperation)) {
				batch.add(record);
			} else {
				// 以每个 collection 加 操作类型为单位，每次保存的都是统一 collection 中的数据并且操作类型相同
				this.writeBatch(batch);

				batch = new ArrayList<>();
				batch.add(record);

				batchCollection = recordCollection;
				batchOperation = recordOperation;
			}

			if (!recordIterator.hasNext()) {
				// 迭代到最后把剩余的数据写入
				this.writeBatch(batch);

				batch = null;
				batchCollection = null;
				batchOperation = null;
			}
		}
	}

	private void writeBatch(final List<ArangoBase> batch) {
		final ArangoBase representativeRecord = batch.get(0);
		final String batchCollection = representativeRecord.collection;
		final String batchOperation = representativeRecord.op;

		switch (batchOperation) {
			case ArangoBase.REPSERT:
				repsertBatch(batchCollection, batch);
				break;
			case ArangoBase.DELETE:
				deleteBatch(batchCollection, batch);
				break;
			default:
		}
	}

	protected ArangoCollection getCollection(final String collection) {
		final ArangoCollection arangoCollection = this.database.collection(collection);
		if (!arangoCollection.exists()) {
			arangoCollection.create(getCollectionOptions());
		}
		return arangoCollection;
	}

	protected abstract CollectionCreateOptions getCollectionOptions();

	protected abstract void deleteBatch(final String collection, final List<ArangoBase> records);

	protected abstract void repsertBatch(final String collection, final List<ArangoBase> records);
}
