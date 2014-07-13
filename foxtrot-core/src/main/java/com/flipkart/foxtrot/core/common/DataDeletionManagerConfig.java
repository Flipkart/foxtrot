package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * Created by rishabh.goyal on 11/07/14.
 */
public class DataDeletionManagerConfig {

    @NotNull
    @NotEmpty
    @JsonProperty("schedule")
    private String deletionSchedule;

    @NotNull
    private boolean active;

    public DataDeletionManagerConfig() {
    }

    public String getDeletionSchedule() {
        return deletionSchedule;
    }

    public void setDeletionSchedule(String deletionSchedule) {
        this.deletionSchedule = deletionSchedule;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
