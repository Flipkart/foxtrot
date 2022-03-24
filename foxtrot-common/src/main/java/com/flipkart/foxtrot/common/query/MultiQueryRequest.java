package com.flipkart.foxtrot.common.query;
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

import com.collections.CollectionUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.junit.Assert;


import java.util.Map;

/***
 Created by nitish.goyal on 22/08/18
 ***/
@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiQueryRequest extends ActionRequest {

    private Map<String, ActionRequest> requests;

    private String consoleId;

    public MultiQueryRequest() {
        super(Opcodes.MULTI_QUERY);
    }

    public MultiQueryRequest(Map<String, ActionRequest> requests,
                             String consoleId) {
        this(requests);
        this.consoleId = consoleId;
    }

    public MultiQueryRequest(Map<String, ActionRequest> requests) {
        super(Opcodes.MULTI_QUERY);
        Assert.assertTrue(CollectionUtils.isNotEmpty(requests));
        this.requests = requests;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("requests", requests.toString())
                .append("consoleId", consoleId)
                .toString();
    }

}
