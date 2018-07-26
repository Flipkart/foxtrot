package com.flipkart.foxtrot.common;

import lombok.Data;
import lombok.ToString;

/**
 * Metadata for a document
 */
@Data
@ToString
public class DocumentMetadata {
    private String id;
    private String rawStorageId;
    private long time;

    public DocumentMetadata() {
    }

    public DocumentMetadata(String id, String rawStorageId, long time) {
        this.id = id;
        this.rawStorageId = rawStorageId;
        this.time = time;
    }

}
