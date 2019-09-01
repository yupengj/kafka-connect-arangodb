package com.gant.kafka.connect.arangodb.writer;

import org.junit.jupiter.api.Test;

import com.gant.kafka.connect.arangodb.entity.ArangoVertex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ArangoRecordTests {

//	private static final String COLLECTION = "md_material";
//	private static final String KEY = "1234";
//	private static final String VALUE = "{\"material_num\":\"1000-05203\"}";
//
//	@Test
//	public void getCollectionReturnsCollection() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertEquals(COLLECTION, record.getCollection());
//	}
//
//	@Test
//	public void getKeyReturnsKey() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertEquals(KEY, record.getKey());
//	}
//
//	@Test
//	public void getValueReturnsValue() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertEquals(VALUE, record.getValue());
//	}
//
//	@Test
//	public void equalsNullReturnsFalse() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertNotEquals(null, record);
//	}
//
//	@Test
//	public void equalsSameObjectReturnsTrue() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertEquals(record, record);
//	}
//
//	@Test
//	public void equalsDifferentObjectTypeReturnsFalse() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		assertNotEquals("something else", record);
//	}
//
//	@Test
//	public void equalsEqualObjectReturnsTrue() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, KEY, VALUE);
//
//		assertEquals(recordA, recordB);
//	}
//
//	@Test
//	public void equalsDifferentCollectionReturnsFalse() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex("DIFFERENT", KEY, VALUE);
//
//		assertNotEquals(recordA, recordB);
//	}
//
//	@Test
//	public void equalsDifferentKeyReturnsFalse() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, "DIFFERENT", VALUE);
//
//		assertNotEquals(recordA, recordB);
//	}
//
//	@Test
//	public void equalsDifferentValueReturnsFalse() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, KEY, "DIFFERENT");
//
//		assertNotEquals(recordA, recordB);
//	}
//
//	@Test
//	public void equalsDifferentValuesReturnsFalse() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex("DIFFERENT", "DIFFERENT", "DIFFERENT");
//		assertNotEquals(recordA, recordB);
//	}
//
//	@Test
//	public void hashCodeSameValuesReturnsSame() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, KEY, VALUE);
//
//		assertEquals(recordA.hashCode(), recordB.hashCode());
//	}
//
//	@Test
//	public void hashCodeDifferentCollectionReturnsDifferent() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex("DIFFERENT", KEY, VALUE);
//
//		assertNotEquals(recordA.hashCode(), recordB.hashCode());
//	}
//
//	@Test
//	public void hashCodeDifferentKeyReturnsDifferent() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, "DIFFERENT", VALUE);
//
//		assertNotEquals(recordA.hashCode(), recordB.hashCode());
//	}
//
//	@Test
//	public void hashCodeDifferentValueReturnsDifferent() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex(COLLECTION, KEY, "DIFFERENT");
//
//		assertNotEquals(recordA.hashCode(), recordB.hashCode());
//	}
//
//	@Test
//	public void hashCodeDifferentValuesReturnsDifferent() {
//		final ArangoVertex recordA = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final ArangoVertex recordB = new ArangoVertex("DIFFERENT", "DIFFERENT", "DIFFERENT");
//
//		assertNotEquals(recordA.hashCode(), recordB.hashCode());
//	}
//
//	@Test
//	public void toStringReturnsStringified() {
//		final ArangoVertex record = new ArangoVertex(COLLECTION, KEY, VALUE);
//		final String expectedStringifiedRecords = "ArangoVertex{collection=" + COLLECTION + ", key=" + KEY + ", value=" + VALUE + "}";
//
//		assertEquals(expectedStringifiedRecords, record.toString());
//	}
}
