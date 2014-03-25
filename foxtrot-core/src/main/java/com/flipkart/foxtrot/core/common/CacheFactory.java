package com.flipkart.foxtrot.core.common;

import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 5:39 PM
 */
public interface CacheFactory<T extends Serializable> {
    public Cache<T> create(String name);
}
