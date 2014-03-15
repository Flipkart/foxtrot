package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/09/13
 * Time: 2:12 PM
 */
public class ClusterConfig {
    @JsonProperty("name")
    @Valid
    @NotNull
    @NotEmpty
    private String name = null;

    @JsonProperty
    private boolean disableMulticast = false;

    @JsonProperty
    private List<String> members = null;

    public ClusterConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDisableMulticast() {
        return disableMulticast;
    }

    public void setDisableMulticast(boolean disableMulticast) {
        this.disableMulticast = disableMulticast;
    }

    public List<String> getMembers() {
        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }
}
