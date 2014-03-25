package com.flipkart.foxtrot.core.common;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:26 AM
 */
public interface Cache<T> {
    public T put(final String key, T data);
    public T get(final String key);
    public boolean has(final String key);
}
