package com.gant.kafka.connect.arangodb.entity;

import java.util.Objects;


public class ArangoVertex extends ArangoBase {

	public final String value;

	public ArangoVertex(String collection, String key, String value) {
		super(collection, key, value == null ? DELETE : REPSERT);
		this.value = value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		ArangoVertex that = (ArangoVertex) o;
		return Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), value);
	}

	@Override
	public String toString() {
		return "ArangoVertex{" + "value='" + value + '\'' + ", collection='" + collection + '\'' + ", key='" + key + '\'' + '}';
	}
}
