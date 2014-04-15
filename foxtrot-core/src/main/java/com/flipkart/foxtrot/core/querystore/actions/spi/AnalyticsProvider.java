package com.flipkart.foxtrot.core.querystore.actions.spi;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.ActionResponse;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 27/03/14
 * Time: 12:15 AM
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AnalyticsProvider {
    public String opcode();
    public Class<? extends ActionRequest> request();
    public Class<? extends ActionResponse> response();
    public boolean cacheable() default false;
}
