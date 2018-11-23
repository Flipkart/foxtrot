package com.flipkart.foxtrot.common;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * Metadata for a document
 */
@Data
@ToString
public class DocumentMetadata implements Serializable {
    private static final long serialVersionUID = -2513729439392513459L;
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
