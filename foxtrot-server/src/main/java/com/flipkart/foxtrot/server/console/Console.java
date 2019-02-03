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
package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Deprecated
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


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("appName", appName)
                .append("updated", updated)
                .append("tileList", tileList)
                .append("tiles", tiles)
                .toString();
    }
}
