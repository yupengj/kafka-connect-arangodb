package com.gant.kafka.connect.arangodb.writer;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDatabase;
import com.gant.kafka.connect.arangodb.entity.ArangoVertex;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WriterTests {
//	@Test
//	public void writeSingleCollectionRepsertWritesRecords() {
//
//		final List<ArangoVertex> arangoVertexStubs = Arrays
//				.asList(new ArangoVertex("somecollection", "aKey", "ajson"), new ArangoVertex("somecollection", "bKey", "bjson"),
//						new ArangoVertex("somecollection", "cKey", "cjson"), new ArangoVertex("somecollection", "dKey", "djson"),
//						new ArangoVertex("somecollection", "eKey", "ejson"));
//
//		final ArangoCollection collectionMock = mock(ArangoCollection.class);
//
//		final ArangoDatabase databaseMock = mock(ArangoDatabase.class);
//		when(databaseMock.collection(any())).thenReturn(collectionMock);
//
//		final VertexWriter vertexWriter = new VertexWriter(databaseMock);
//		vertexWriter.write(arangoVertexStubs);
//
//		verify(collectionMock).insertDocuments(eq(Arrays.asList("ajson", "bjson", "cjson", "djson", "ejson")),
//				argThat(options -> (options.getOverwrite()) && (options.getWaitForSync()) && (options.getSilent())));
//
//		verify(collectionMock, never()).deleteDocuments(any());
//	}
//
//	@Test
//	public void writeMultipleCollectionRepsertWritesRecords() {
//		// Set up input stubs
//		final List<ArangoVertex> arangoVertexStubs = Arrays
//				.asList(new ArangoVertex("somecollection", "aKey", "ajson"), new ArangoVertex("somecollection", "bKey", "bjson"),
//						new ArangoVertex("othercollection", "cKey", "cjson"), new ArangoVertex("othercollection", "dKey", "djson"),
//						new ArangoVertex("elsecollection", "eKey", "ejson"));
//
//		// Set up VertexWriter dependencies
//		final ArangoCollection collectionMock = mock(ArangoCollection.class);
//
//		final ArangoDatabase databaseMock = mock(ArangoDatabase.class);
//		when(databaseMock.collection(any())).thenReturn(collectionMock);
//
//		// Test system under test
//		final VertexWriter vertexWriter = new VertexWriter(databaseMock);
//		vertexWriter.write(arangoVertexStubs);
//
//		InOrder inOrder = Mockito.inOrder(collectionMock);
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("ajson", "bjson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("cjson", "djson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("ejson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//
//		verify(collectionMock, never()).deleteDocuments(any());
//	}
//
//	@Test
//	public void writeSingleCollectionDeleteWritesRecords() {
//		// Set up input stubs
//		final List<ArangoVertex> arangoVertexStubs = Arrays
//				.asList(new ArangoVertex("somecollection", "aKey", null), new ArangoVertex("somecollection", "bKey", null),
//						new ArangoVertex("somecollection", "cKey", null), new ArangoVertex("somecollection", "dKey", null),
//						new ArangoVertex("somecollection", "eKey", null));
//
//		// Set up VertexWriter dependencies
//		final ArangoCollection collectionMock = mock(ArangoCollection.class);
//
//		final ArangoDatabase databaseMock = mock(ArangoDatabase.class);
//		when(databaseMock.collection(any())).thenReturn(collectionMock);
//
//		// Test system under test
//		final VertexWriter vertexWriter = new VertexWriter(databaseMock);
//		vertexWriter.write(arangoVertexStubs);
//
//		verify(collectionMock).deleteDocuments(eq(Arrays.asList("aKey", "bKey", "cKey", "dKey", "eKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//
//		verify(collectionMock, never()).insertDocuments(any());
//	}
//
//	@Test
//	public void writeMultipleCollectionDeleteWritesRecords() {
//		// Set up input stubs
//		final List<ArangoVertex> arangoVertexStubs = Arrays
//				.asList(new ArangoVertex("somecollection", "aKey", null), new ArangoVertex("somecollection", "bKey", null),
//						new ArangoVertex("othercollection", "cKey", null), new ArangoVertex("othercollection", "dKey", null),
//						new ArangoVertex("elsecollection", "eKey", null));
//
//		// Set up VertexWriter dependencies
//		final ArangoCollection collectionMock = mock(ArangoCollection.class);
//
//		final ArangoDatabase databaseMock = mock(ArangoDatabase.class);
//		when(databaseMock.collection(any())).thenReturn(collectionMock);
//
//		// Test system under test
//		final VertexWriter vertexWriter = new VertexWriter(databaseMock);
//		vertexWriter.write(arangoVertexStubs);
//
//		InOrder inOrder = Mockito.inOrder(collectionMock);
//		inOrder.verify(collectionMock).deleteDocuments(eq(Arrays.asList("aKey", "bKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).deleteDocuments(eq(Arrays.asList("cKey", "dKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).deleteDocuments(eq(Arrays.asList("eKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//
//		verify(collectionMock, never()).insertDocuments(any());
//	}
//
//	@Test
//	public void writeMixedWritesRecords() {
//		// Set up input stubs
//		final List<ArangoVertex> arangoVertexStubs = Arrays
//				.asList(new ArangoVertex("somecollection", "aKey", "ajson"), new ArangoVertex("somecollection", "bKey", "bjson"),
//						new ArangoVertex("othercollection", "cKey", "cjson"), new ArangoVertex("othercollection", "dKey", null),
//						new ArangoVertex("somecollection", "eKey", null), new ArangoVertex("somecollection", "fKey", null),
//						new ArangoVertex("somecollection", "gKey", "gjson"));
//
//		// Set up VertexWriter dependencies
//		final ArangoCollection collectionMock = mock(ArangoCollection.class);
//
//		final ArangoDatabase databaseMock = mock(ArangoDatabase.class);
//		when(databaseMock.collection(any())).thenReturn(collectionMock);
//
//		// Test system under test
//		final VertexWriter vertexWriter = new VertexWriter(databaseMock);
//		vertexWriter.write(arangoVertexStubs);
//
//		InOrder inOrder = Mockito.inOrder(collectionMock);
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("ajson", "bjson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("cjson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).deleteDocuments(eq(Arrays.asList("dKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).deleteDocuments(eq(Arrays.asList("eKey", "fKey")), eq(null), argThat(options -> {
//			return (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//		inOrder.verify(collectionMock).insertDocuments(eq(Arrays.asList("gjson")), argThat(options -> {
//			return (options.getOverwrite() == true) && (options.getWaitForSync() == true) && (options.getSilent() == true);
//		}));
//	}
}
