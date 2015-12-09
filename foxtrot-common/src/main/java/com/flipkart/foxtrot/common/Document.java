/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:17 PM
 */
public class Document implements Serializable {
    @NotNull
    @NotEmpty
    @JsonProperty
    private String id;

    @JsonProperty
    private long timestamp;

    private DocumentMetadata metadata;

    @NotNull
    @JsonProperty
    private JsonNode data;

    public Document() {
        this.timestamp = System.currentTimeMillis();
    }

    public Document(String id, long timestamp, JsonNode data) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
    }

    public Document(String id, long timestamp, DocumentMetadata metadata, JsonNode data) {
        this.id = id;
        this.timestamp = timestamp;
        this.metadata = metadata;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JsonNode getData() {
        return data;
    }

    public void setData(JsonNode data) {
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public DocumentMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(DocumentMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", metadata=" + metadata +
                ", data=" + data +
                '}';
    }
}
