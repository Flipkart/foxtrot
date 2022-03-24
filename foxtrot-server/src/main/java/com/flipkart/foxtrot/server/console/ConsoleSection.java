package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsoleSection {

    @NotNull
    @NotEmpty
    private String id;

    @NotNull
    @NotEmpty
    private String name;

    private List<String> tileList;

    private Map<String, ConsoleTile> tileData;

    private TemplateFilter templateFilter;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", id)
                .append("name", name)
                .append("tileList", tileList)
                .append("tileData", tileData)
                .append("templateFilter", templateFilter)
                .toString();
    }
}
