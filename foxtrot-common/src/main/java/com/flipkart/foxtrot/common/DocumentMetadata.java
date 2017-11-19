package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Metadata for a document
 */
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

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "DocumentMetadata{" +
                "id='" + id + '\'' +
                ", rawStorageId='" + rawStorageId + '\'' +
                ", time=" + time +
                '}';
    }
}
