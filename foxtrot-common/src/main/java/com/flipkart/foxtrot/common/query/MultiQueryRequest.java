package com.flipkart.foxtrot.common.query;
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

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionRequestVisitor;
import com.flipkart.foxtrot.common.Opcodes;
import java.util.Map;
import lombok.Data;
import org.junit.Assert;

/***
 Created by nitish.goyal on 22/08/18
 ***/
@Data
public class MultiQueryRequest extends ActionRequest {

    private Map<String, ActionRequest> requests;

    public MultiQueryRequest() {
        super(Opcodes.MULTI_QUERY);
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
        return requests.toString();
    }

}
