/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * Representation for a table on foxtrot.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Table implements Serializable {

    @JsonIgnore
    private static final long serialVersionUID = -3086868483579299018L;

    @NotNull
    @NotEmpty
    private String name;

    @Min(1)
    @Max(180)
    private int ttl;

    private boolean seggregatedBackend = false;

    @Min(1)
    @Max(256)
    private int defaultRegions = 4;

    @Builder
    public Table(String name, int ttl, boolean seggregatedBackend, int defaultRegions) {
        this.name = name;
        this.ttl = ttl;
        this.seggregatedBackend = seggregatedBackend;
        if (defaultRegions == 0) {
            defaultRegions = 4;
        }
        this.defaultRegions = defaultRegions;
    }
}
