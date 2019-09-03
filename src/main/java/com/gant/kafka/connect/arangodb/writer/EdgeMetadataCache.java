package com.gant.kafka.connect.arangodb.writer;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;

public class EdgeMetadataCache {
	private static final Logger LOGGER = LoggerFactory.getLogger(EdgeMetadataCache.class);

	private final static Set<EdgeMetadata> relationCache = new HashSet<>();
	private final static Set<EdgeMetadata> removeRelation = new HashSet<>();

	public final Set<EdgeMetadata> getCache() {
		return relationCache;
	}

	public final void clear() {
		LOGGER.info("EdgeMetadataCache clear size {}", relationCache.size());
		relationCache.clear();
	}


	public void add(EdgeMetadata edgeMetadata) {
		if (edgeMetadata != null) {
			if (edgeMetadata.getEdgeCollection() == null) {// 删除元数据操作
				// 查找要删除的元数据，如果没有找到忽略
				EdgeMetadata delete = relationCache.stream().filter(it -> it.getKey().equals(edgeMetadata.getKey())).findFirst().orElse(null);
				if (delete == null) {
					return;
				}
				LOGGER.info(" remove edgeMetadata {}", edgeMetadata);
				relationCache.remove(delete); // 把找到的记录从缓存中删除
				removeRelation.add(delete); // 把删除的元数据放入删除集合
			} else {
				LOGGER.info(" add edgeMetadata {}", edgeMetadata);
				relationCache.add(edgeMetadata);
			}
		}
	}

	public final Set<EdgeMetadata> getRemoveRelation() {
		return removeRelation;
	}

}
