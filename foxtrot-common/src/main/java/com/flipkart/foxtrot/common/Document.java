package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:17 PM
 */
public class Document {
    @JsonProperty
    private String id;

    @JsonProperty
    private JsonNode data;

    public Document(String id, JsonNode data) {
        this.id = id;
        this.data = data;
    }

    public Document() {
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
}
