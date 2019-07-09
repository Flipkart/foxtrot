package com.flipkart.foxtrot.core.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by rishabh.goyal on 11/07/14.
 */
public class DataDeletionManagerConfig {

    @Min(3600)
    private int interval;

    @Min(1)
    @JsonProperty("initialdelay")
    private int initialDelay;

    @NotNull
    private boolean active;

    public DataDeletionManagerConfig() {
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public int getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(int initialDelay) {
        this.initialDelay = initialDelay;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
