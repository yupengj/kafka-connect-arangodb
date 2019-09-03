package com.gant.kafka.connect.arangodb.entity;

import java.util.Objects;

public class EdgeMetadata {

	public final static String KEY = "key"; // 这里增加key 对应关系数据库中的主键值，因为删除边关系时，record value 没有值。只能从 key 判断删除了那条记录
	public final static String EDGE_COLLECTION = "edgeCollection";
	public final static String FROM_COLLECTION = "fromCollection";
	public final static String FROM_ATTRIBUTE = "fromAttribute";
	public final static String TO_COLLECTION = "toCollection";
	public final static String TO_ATTRIBUTE = "toAttribute";

	private String key; // 这里增加key 对应关系数据库中的主键值，因为删除边关系时，record value 没有值。只能从 key 判断删除了那条记录
	private String edgeCollection;
	private String fromCollection;
	private String fromAttribute;
	private String toCollection;
	private String toAttribute;

	public EdgeMetadata() {
	}

	public EdgeMetadata(String key, String edgeCollection, String fromCollection, String fromAttribute, String toCollection, String toAttribute) {
		this.key = key;
		this.edgeCollection = edgeCollection;
		this.fromCollection = fromCollection;
		this.fromAttribute = fromAttribute;
		this.toCollection = toCollection;
		this.toAttribute = toAttribute;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getEdgeCollection() {
		return edgeCollection;
	}

	public void setEdgeCollection(String edgeCollection) {
		this.edgeCollection = edgeCollection;
	}

	public String getFromCollection() {
		return fromCollection;
	}

	public void setFromCollection(String fromCollection) {
		this.fromCollection = fromCollection;
	}

	public String getFromAttribute() {
		return fromAttribute;
	}

	public void setFromAttribute(String fromAttribute) {
		this.fromAttribute = fromAttribute;
	}

	public String getToCollection() {
		return toCollection;
	}

	public void setToCollection(String toCollection) {
		this.toCollection = toCollection;
	}

	public String getToAttribute() {
		return toAttribute;
	}

	public void setToAttribute(String toAttribute) {
		this.toAttribute = toAttribute;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		EdgeMetadata that = (EdgeMetadata) o;
		return Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key);
	}

	@Override
	public String toString() {
		return "EdgeMetadata{" + "key='" + key + '\'' + ", edgeCollection='" + edgeCollection + '\'' + ", fromCollection='" + fromCollection + '\''
				+ ", fromAttribute='" + fromAttribute + '\'' + ", toCollection='" + toCollection + '\'' + ", toAttribute='" + toAttribute + '\'' + '}';
	}
}
