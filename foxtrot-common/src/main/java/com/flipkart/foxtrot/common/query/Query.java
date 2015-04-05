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
package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionRequest;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 6:38 PM
 */
public class Query extends ActionRequest {
    @NotNull
    private String table;

    private ResultSort sort;

    @Min(0)
    private int from = 0;

    @Min(10)
    private int limit = 10;

    public Query() {
        this.sort = new ResultSort();
        this.sort.setField("_timestamp");
        this.sort.setOrder(ResultSort.Order.desc);
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
        return new ToStringBuilder(this)
                .append("table", table)
                .append("filters", getFilters())
                .append("sort", sort)
                .append("from", from)
                .append("limit", limit)
                .toString();
    }
}
