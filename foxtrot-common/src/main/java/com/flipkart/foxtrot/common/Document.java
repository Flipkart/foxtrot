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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.foxtrot.common.util.Utils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;
import org.joda.time.DateTime;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.UUID;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com) Date: 12/03/14 Time: 9:17 PM
 */

@Data
@EqualsAndHashCode
@ToString
public class Document implements Serializable {

    private static final long serialVersionUID = 1394184997687819635L;

    @NotNull
    @NotEmpty
    @JsonProperty
    private String id;

    @JsonProperty
    private long timestamp;

    private Date date;

    private DocumentMetadata metadata;

    @NotNull
    @JsonProperty
    private JsonNode data;

    public Document() {
        this.id = UUID.randomUUID()
                .toString();
        this.timestamp = System.currentTimeMillis();
        this.date = new Date(DateTime.now());
    }

    public Document(String id, long timestamp, JsonNode data) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
        this.date = Utils.getDate(timestamp);
    }

    public Document(String id, long timestamp, JsonNode data, Date date) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
        this.date = date;
    }

    @Builder
    public Document(String id, long timestamp, DocumentMetadata metadata, JsonNode data) {
        this.id = id;
        this.timestamp = timestamp;
        this.metadata = metadata;
        this.data = data;
        this.date = Utils.getDate(timestamp);
    }

}
