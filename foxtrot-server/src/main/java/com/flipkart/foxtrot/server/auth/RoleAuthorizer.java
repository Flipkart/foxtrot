package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.core.auth.FoxtrotRole;
import io.dropwizard.auth.Authorizer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Singleton;

/**
 *
 */
@Singleton
@Slf4j
public class RoleAuthorizer implements Authorizer<UserPrincipal> {
    @Override
    public boolean authorize(UserPrincipal userPrincipal, String role) {
        val user = userPrincipal.getUser();
        val foxtrotRole = FoxtrotRole.valueOf(role);

        if(!user.getRoles().contains(foxtrotRole)) {
            log.warn("User {} is trying to access unauthorized role: {}", user.getId(), role);
            return false;
        }
        val token = userPrincipal.getToken();
        if(!token.getTokenType().getAllowedRoles().contains(foxtrotRole)) {
            log.warn("User {} trying to access resource with role: {} with token of tye: {}",
                     user.getId(), role, token.getTokenType().name());
            return false;
        }
        return true;
    }
}
