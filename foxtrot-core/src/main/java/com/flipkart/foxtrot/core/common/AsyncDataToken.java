package com.flipkart.foxtrot.core.common;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 26/03/14
 * Time: 7:02 PM
 */
public class AsyncDataToken {
    private String action;
    private String key;

    public AsyncDataToken(String action, String key) {
        this.action = action;
        this.key = key;
    }

    public AsyncDataToken() {
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
