package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.auth.authprovider.AuthProviderConfig;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 *
 */
@Data
public class AuthConfig {

    @NotNull
    @Valid
    public AuthProviderConfig provider;
    private boolean enabled;
    @NotNull
    @Valid
    private JwtConfig jwt = new JwtConfig();
}
