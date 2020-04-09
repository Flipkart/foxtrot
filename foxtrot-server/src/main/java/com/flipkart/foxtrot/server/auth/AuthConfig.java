package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.auth.authprovider.AuthProviderConfig;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 *
 */
@Data
public class AuthConfig {
    private boolean enabled;

    @NotNull
    @Valid
    private JwtConfig jwt = new JwtConfig();

    @NotNull
    @Valid
    public AuthProviderConfig provider;
}
