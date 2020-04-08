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
package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 1:00 PM
 */
@Data
public class QueryResponse extends ActionResponse {
    private List<Document> documents;
    private long totalHits;

    private String scrollId;
    private boolean moreDataAvailable;

    public QueryResponse() {
        super(Opcodes.QUERY);
    }

    @Builder
    public QueryResponse(List<Document> documents, long totalHits, String scrollId, boolean moreDataAvailable) {
        super(Opcodes.QUERY);
        this.documents = documents;
        this.totalHits = totalHits;
        this.scrollId = scrollId;
        this.moreDataAvailable = moreDataAvailable;
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }

}
