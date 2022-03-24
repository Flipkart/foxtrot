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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.ResultSort;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Map;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com) Date: 13/03/14 Time: 6:38 PM
 */
@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Query extends ActionRequest {

    private String table;

    private ResultSort sort;

    private int from = 0;

    private int limit = 10;

    private boolean scrollRequest = false;
    private String scrollId;

    public Query() {
        super(Opcodes.QUERY);
        this.sort = new ResultSort();
        this.sort.setField("_timestamp");
        this.sort.setOrder(ResultSort.Order.desc);
    }

    public Query(List<Filter> filters,
                 String table,
                 ResultSort sort,
                 int from,
                 int limit,
                 boolean bypassCache,
                 Map<String, String> requestTags,
                 SourceType sourceType,
                 boolean extrapolationFlag) {
        super(Opcodes.QUERY, filters, bypassCache, requestTags, sourceType, extrapolationFlag);
        this.table = table;
        this.sort = sort;
        this.from = from;
        this.limit = limit;
    }

    public <T> T accept(ActionRequestVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public ResultSort getSort() {
        return sort;
    }

    public void setSort(ResultSort sort) {
        this.sort = sort;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("table", table)
                .append("filters", getFilters())
                .append("sort", sort)
                .append("from", from)
                .append("limit", limit)
                .append("scroll", scrollRequest)
                .append("scrollId", scrollId)
                .toString();
    }

}
