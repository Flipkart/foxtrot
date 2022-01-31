package com.flipkart.foxtrot.core.auth;

import lombok.Value;

import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

/**
 *
 */
@Value
public class User {
    public static final User DEFAULT
            = new User("__DEFAULT__", EnumSet.allOf(FoxtrotRole.class), Collections.emptySet(), true, new Date(), new Date());

    String id;
    Set<FoxtrotRole> roles;
    Set<String> tables;
    boolean systemUser;
    Date created;
    Date updated;
}
