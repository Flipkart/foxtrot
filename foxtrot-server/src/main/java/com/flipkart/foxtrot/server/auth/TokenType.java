package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import lombok.Getter;

import java.util.EnumSet;
import java.util.Set;

/**
 *
 */
@Getter
public enum TokenType {
    DYNAMIC(EnumSet.of(FoxtrotRole.QUERY, FoxtrotRole.CONSOLE)),
    STATIC(EnumSet.of(FoxtrotRole.QUERY, FoxtrotRole.INGEST)),
    SYSTEM(EnumSet.allOf(FoxtrotRole.class));

    private final Set<FoxtrotRole> allowedRoles;

    TokenType(Set<FoxtrotRole> allowedRoles) {
        this.allowedRoles = allowedRoles;
    }
}
