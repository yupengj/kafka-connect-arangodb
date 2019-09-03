package com.gant.kafka.connect.arangodb.entity;

import java.util.Objects;

import com.arangodb.entity.BaseEdgeDocument;

public class ArangoEdge extends ArangoBase {

	public final BaseEdgeDocument baseEdgeDocument;

	public ArangoEdge(String collection, String from, String to) {
		super(collection, from.replace("/", "_"), to == null ? DELETE : REPSERT); // 把的 “/” 转成 “_” 作为边的 key。 md_material/123 --  md_material_123。用于边的更新
		this.baseEdgeDocument = new BaseEdgeDocument(key, from, to);
	}

	public String getFrom() {
		return this.baseEdgeDocument.getFrom();
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
		ArangoEdge that = (ArangoEdge) o;
		return Objects.equals(baseEdgeDocument, that.baseEdgeDocument);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), baseEdgeDocument);
	}

	@Override
	public String toString() {
		return "ArangoEdge{" + "baseEdgeDocument=" + baseEdgeDocument + ", collection='" + collection + '\'' + ", key='" + key + '\'' + ", op='" + op + '\''
				+ '}';
	}
}
