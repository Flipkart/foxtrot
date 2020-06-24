package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import java.util.EnumSet;
import java.util.Set;
import lombok.Getter;

/**
 *
 */
@Getter
public enum TokenType {
    DYNAMIC(EnumSet.of(FoxtrotRole.QUERY, FoxtrotRole.CONSOLE, FoxtrotRole.SYSADMIN)),
    STATIC(EnumSet.of(FoxtrotRole.QUERY, FoxtrotRole.INGEST)),
    SYSTEM(EnumSet.allOf(FoxtrotRole.class));

    private final Set<FoxtrotRole> allowedRoles;

    TokenType(Set<FoxtrotRole> allowedRoles) {
        this.allowedRoles = allowedRoles;
    }
}
