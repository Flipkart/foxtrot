package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * Metadata for a document
 */
@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
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

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", id)
                .append("rawStorageId", rawStorageId)
                .toString();
    }
}
