package com.flipkart.foxtrot.server.auth;

import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 *
 */
@Data
public class AuthConfig {
    private boolean disabled;
    @NotNull
    @Valid
    private JwtConfig jwt = new JwtConfig();
}
