package com.gant.kafka.connect.arangodb.entity;

import java.util.Objects;

public class ArangoBase {
	public final static String DELETE = "delete";
	public final static String REPSERT = "repsert";

	public final String collection;
	public final String key;
	public final String op;

	public ArangoBase(String collection, String key, String op) {
		this.collection = collection;
		this.key = key;
		this.op = op;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ArangoBase that = (ArangoBase) o;
		return Objects.equals(collection, that.collection) && Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return Objects.hash(collection, key);
	}

	@Override
	public String toString() {
		return "ArangoBase{" + "collection='" + collection + '\'' + ", key='" + key + '\'' + '}';
	}
}
