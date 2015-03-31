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
package com.flipkart.foxtrot.common;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:51 PM
 */
public class Table implements Serializable {

    private static final long serialVersionUID = -3086868483579299018L;
    
    @NotNull
    @NotEmpty
    private String name;

    @Min(1)
    @Max(180)
    private int ttl;

    private boolean seggregatedBackend = false;

    public Table() {
    }

    public Table(String name, int ttl) {
        this.name = name;
        this.ttl = ttl;
    }

    public Table(String name, int ttl, boolean seggregatedBackend) {
        this.name = name;
        this.ttl = ttl;
        this.seggregatedBackend = seggregatedBackend;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("name", name)
                .append("ttl", ttl)
                .append("seggregatedBackend", seggregatedBackend)
                .toString();
    }

    public boolean isSeggregatedBackend() {
        return seggregatedBackend;
    }

    public void setSeggregatedBackend(boolean seggregatedBackend) {
        this.seggregatedBackend = seggregatedBackend;
    }

}
