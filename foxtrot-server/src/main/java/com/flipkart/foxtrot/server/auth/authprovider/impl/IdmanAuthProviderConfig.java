package com.flipkart.foxtrot.server.auth.authprovider.impl;

import com.flipkart.foxtrot.server.auth.authprovider.AuthConfigVisitor;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.AuthType;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;

/**
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class IdmanAuthProviderConfig extends AuthProviderConfig {
    @NotEmpty
    public String serverEndpoint;
    @NotEmpty
    private String idmanEndpoint;
    @NotEmpty
    private String clientId;
    @NotEmpty
    private String clientSecret;

    public IdmanAuthProviderConfig() {
        super(AuthType.OAUTH_IDMAN);
    }

    @Builder
    public IdmanAuthProviderConfig(
            AuthType type,
            boolean enabled,
            String idmanEndpoint,
            String clientId,
            String clientSecret) {
        super(type, enabled);
        this.idmanEndpoint = idmanEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    public <T> T accept(AuthConfigVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
