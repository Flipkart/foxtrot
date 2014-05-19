package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class Console {
    @NotNull
    @NotEmpty
    @JsonProperty
    private String id;

    @NotNull
    @NotEmpty
    @JsonProperty
    private String name;

    @JsonProperty
    private String appName;

    @JsonProperty
    private long updated;

    @JsonProperty
    private List<String> tileList;

    @JsonProperty
    private Map<String, Object> tiles;

    public Console() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getUpdated() {
        return updated;
    }

    public void setUpdated(long updated) {
        this.updated = updated;
    }

    public List<String> getTileList() {
        return tileList;
    }

    public void setTileList(List<String> tileList) {
        this.tileList = tileList;
    }

    public Map<String, Object> getTiles() {
        return tiles;
    }

    public void setTiles(Map<String, Object> tiles) {
        this.tiles = tiles;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
