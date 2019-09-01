package com.gant.kafka.connect.arangodb.entity;

import java.util.Objects;

public class EdgeMetadata {
	public final String key; // 这里增加key 对应关系数据库中的主键值，因为删除边关系时，record value 没有值。只能从 key 判断删除了那条记录
	public final String edgeCollection;
	public final String fromCollection;
	public final String fromAttribute;
	public final String toCollection;
	public final String toAttribute;


	public EdgeMetadata(String key, String edgeCollection, String fromCollection, String fromAttribute, String toCollection, String toAttribute) {
		this.key = key;
		this.edgeCollection = edgeCollection;
		this.fromCollection = fromCollection;
		this.fromAttribute = fromAttribute;
		this.toCollection = toCollection;
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
}
