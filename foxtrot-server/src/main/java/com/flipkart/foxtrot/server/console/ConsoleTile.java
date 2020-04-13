package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsoleTile {

    private String id;
    private String title;
    private String desc;
    private Map<String, Object> tileContext;
    private List<ConsoleSection> children;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", id)
                .append("title", title)
                .append("desc", desc)
                .append("tileContext", tileContext)
                .append("children", children)
                .toString();
    }
}
