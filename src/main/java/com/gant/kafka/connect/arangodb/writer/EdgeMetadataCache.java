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

	public void add(EdgeMetadata edgeMetadata) {
		if (edgeMetadata != null) {
			if (edgeMetadata.getEdgeCollection() == null) {// 删除元数据操作
				LOGGER.info(" remove edgeMetadata {}", edgeMetadata);
				relationCache.remove(edgeMetadata);
				removeRelation.add(edgeMetadata);
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
