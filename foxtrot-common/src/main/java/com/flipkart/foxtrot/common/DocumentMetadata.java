package com.flipkart.foxtrot.common;

/**
 * Metadata for a document
 */
public class DocumentMetadata {
    private String id;
    private String rowKey;

    public DocumentMetadata() {
    }

    public DocumentMetadata(String id, String rowKey) {
        this.id = id;
        this.rowKey = rowKey;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public String toString() {
        return "DocumentMetadata{" +
                "id='" + id + '\'' +
                ", rowKey='" + rowKey + '\'' +
                '}';
    }
}
