/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Representation for a table on foxtrot.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table implements Serializable {

    @JsonIgnore
    public static final int DEFAULT_COLUMNS = 5000;
    @JsonIgnore
    private static final long serialVersionUID = -3086868483579299018L;
    @NotNull
    @NotEmpty
    private String name;

    @Min(1)
    private int ttl;

    private boolean seggregatedBackend = false;

    @Min(1)
    private int shards;

    @Min(DEFAULT_COLUMNS)
    private int columns;

    private SortedMap<String, FieldDataType> customFieldMappings = new TreeMap<>();

    private int refreshIntervalInSecs = 30;

    @Min(1)
    @Max(256)
    private int defaultRegions = 4;

    @NotNull
    @NotEmpty
    private String tenantName;

    private String defaultPipeline;

    @Builder
    public Table(String name,
                 int ttl,
                 boolean seggregatedBackend,
                 int defaultRegions,
                 String tenantName,
                 int shards,
                 int columns,
                 SortedMap<String, FieldDataType> customFieldMappings,
                 int refreshIntervalInSecs,
                 String defaultPipeline) {

        this.name = name;
        this.ttl = ttl;
        this.shards = shards;
        this.seggregatedBackend = seggregatedBackend;
        if (defaultRegions == 0) {
            defaultRegions = 4;
        }

        if (refreshIntervalInSecs == 0) {
            refreshIntervalInSecs = 30;
        }
        this.columns = columns;
        this.customFieldMappings = customFieldMappings;
        this.defaultRegions = defaultRegions;
        this.refreshIntervalInSecs = refreshIntervalInSecs;
        this.tenantName = tenantName;
        this.defaultPipeline = defaultPipeline;
    }
}
