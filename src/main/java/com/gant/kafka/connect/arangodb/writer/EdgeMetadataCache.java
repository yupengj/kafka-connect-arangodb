package com.gant.kafka.connect.arangodb.writer;

import java.util.HashSet;
import java.util.Set;

import com.gant.kafka.connect.arangodb.entity.EdgeMetadata;

public class EdgeMetadataCache {

	private final static Set<EdgeMetadata> relationCache = new HashSet<>();
	private final static Set<EdgeMetadata> removeRelation = new HashSet<>();

	public final Set<EdgeMetadata> getCache() {
		return relationCache;
	}

	public void add(EdgeMetadata edgeMetadata) {
		if (edgeMetadata != null) {
			relationCache.add(edgeMetadata);
		}
	}

	public void remove(EdgeMetadata edgeMetadata) {
		if (edgeMetadata != null) {
			relationCache.remove(edgeMetadata);
			removeRelation.add(edgeMetadata);
		}
	}

	public final Set<EdgeMetadata> getRemoveRelation() {
		return removeRelation;
	}

}
