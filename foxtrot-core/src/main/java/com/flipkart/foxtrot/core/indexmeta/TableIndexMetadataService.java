package com.flipkart.foxtrot.core.indexmeta;

import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.indexmeta.model.TableIndexMetadata;

import java.util.List;

public interface TableIndexMetadataService {

    void syncTableIndexMetadata(int oldMetadataSyncDays);

    TableIndexMetadata getIndexMetadata(String indexName);

    void cleanupIndexMetadata(int retentionDays);

    List<TableIndexMetadata> getAllIndicesMetadata();

    List<TableIndexMetadata> searchIndexMetadata(List<Filter> filters);

    List<TableIndexMetadata> getTableIndicesMetadata(String table);
}
