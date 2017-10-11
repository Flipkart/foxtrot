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
package com.flipkart.foxtrot.common.group;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.query.Filter;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 21/03/14
 * Time: 4:52 PM
 */
public class GroupRequest extends ActionRequest {

    private String table;

    private String uniqueCountOn;

    private List<String> nesting;

    public GroupRequest() {
        super(Opcodes.GROUP);
    }

    public GroupRequest(List<Filter> filters, String table, String uniqueCountOn, List<String> nesting) {
        super(Opcodes.GROUP, filters);
        this.table = table;
        this.uniqueCountOn = uniqueCountOn;
        this.nesting = nesting;
    }

    public String getTable() {
        return table;
    }

    public String getUniqueCountOn() {
        return uniqueCountOn;
    }

    public void setUniqueCountOn(String uniqueCountOn) {
        this.uniqueCountOn = uniqueCountOn;
    }

    public void setTable(String table) {
        this.table = table;
    }


    public List<String> getNesting() {
        return nesting;
    }

    public void setNesting(List<String> nesting) {
        this.nesting = nesting;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("table", table)
                .append("uniqueCountOn", uniqueCountOn)
                .append("nesting", nesting)
                .toString();
    }
}
