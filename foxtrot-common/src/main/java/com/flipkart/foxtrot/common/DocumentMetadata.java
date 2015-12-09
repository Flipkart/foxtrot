package com.flipkart.foxtrot.common;

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
        return "DocumentMetadata{" +
                "id='" + id + '\'' +
                ", rawStorageId='" + rawStorageId + '\'' +
                '}';
    }
}
