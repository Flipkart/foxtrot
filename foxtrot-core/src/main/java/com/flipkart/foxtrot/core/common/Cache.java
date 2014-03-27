package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.common.ActionResponse;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:26 AM
 */
public interface Cache {
    public ActionResponse put(final String key, ActionResponse data);
    public ActionResponse get(final String key);
    public boolean has(final String key);
}
