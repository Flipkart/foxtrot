package com.flipkart.foxtrot.core.common;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 5:39 PM
 */
public interface CacheFactory {
    public Cache create(String name);
}
