package com.flipkart.foxtrot.common.query;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 14/03/14
 * Time: 9:00 PM
 */
public class ResultSort {
    public static enum Order {
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


}
