package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Metadata for a document
 */
public class DocumentMetadata {
    private String id;
    private String rawStorageId;

    public DocumentMetadata() {
    }

    public DocumentMetadata(String id, String rawStorageId) {
        this.id = id;
        this.rawStorageId = rawStorageId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRawStorageId() {
        return rawStorageId;
    }

    public void setRawStorageId(String rawStorageId) {
        this.rawStorageId = rawStorageId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("rawStorageId", rawStorageId)
                .toString();
    }
}
