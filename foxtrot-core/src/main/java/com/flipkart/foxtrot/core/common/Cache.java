package com.flipkart.foxtrot.core.common;

import java.io.Serializable;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:26 AM
 */
public interface Cache<T extends Serializable> {
    public T put(final String key, T data);
    public T get(final String key);
    public boolean has(final String key);
}
