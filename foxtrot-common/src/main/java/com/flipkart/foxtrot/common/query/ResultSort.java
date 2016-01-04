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

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 9:00 PM
 */
public class ResultSort {
    public enum Order {
        asc,
        desc
    }
    private String field;
    private Order order = Order.desc;

    public ResultSort() {
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResultSort that = (ResultSort) o;

        if (!field.equals(that.field)) return false;
        if (order != that.order) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + order.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ResultSort{" +
                "field='" + field + '\'' +
                ", order=" + order +
                '}';
    }
}
