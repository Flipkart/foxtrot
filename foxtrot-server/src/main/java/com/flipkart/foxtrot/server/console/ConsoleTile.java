package com.flipkart.foxtrot.server.console;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Map;

public class ConsoleTile {

    private String id;
    private String title;
    private Map<String, Object> tileContext;
    private List<ConsoleSection> children;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Map<String, Object> getTileContext() {
        return tileContext;
    }

    public void setTileContext(Map<String, Object> tileContext) {
        this.tileContext = tileContext;
    }

    public List<ConsoleSection> getChildren() {
        return children;
    }

    public void setChildren(List<ConsoleSection> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("title", title)
                .append("tileContext", tileContext)
                .append("children", children)
                .toString();
    }
}
