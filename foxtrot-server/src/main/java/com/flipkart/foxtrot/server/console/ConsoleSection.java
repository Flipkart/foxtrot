package com.flipkart.foxtrot.server.console;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public class ConsoleSection {

    @NotNull
    @NotEmpty
    private String id;

    @NotNull
    @NotEmpty
    private String name;

    private List<String> tileList;

    private Map<String, ConsoleTile> tileData;

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

    public List<String> getTileList() {
        return tileList;
    }

    public void setTileList(List<String> tileList) {
        this.tileList = tileList;
    }

    public Map<String, ConsoleTile> getTileData() {
        return tileData;
    }

    public void setTileData(Map<String, ConsoleTile> tileData) {
        this.tileData = tileData;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("tileList", tileList)
                .append("tileData", tileData)
                .toString();
    }
}
