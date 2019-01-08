package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * Metadata for a document
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DocumentMetadata implements Serializable {
    private static final long serialVersionUID = -2513729439392513459L;
    private String id;
    private String rawStorageId;

    public DocumentMetadata() {
    }

    public DocumentMetadata(String id, String rawStoDocumentMetadatarageId) {
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
        return new ToStringBuilder(this).append("id", id)
                .append("rawStorageId", rawStorageId)
                .toString();
    }
}
