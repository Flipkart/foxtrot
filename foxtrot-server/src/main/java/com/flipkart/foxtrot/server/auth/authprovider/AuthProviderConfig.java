package com.flipkart.foxtrot.server.auth.authprovider;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.impl.IdmanAuthProviderConfig;
import lombok.Data;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "OAUTH_GOOGLE", value = GoogleAuthProviderConfig.class),
        @JsonSubTypes.Type(name = "OAUTH_IDMAN", value = IdmanAuthProviderConfig.class),
})
@Data
public abstract class AuthProviderConfig {
    private final AuthType type;

    private boolean enabled;

    protected AuthProviderConfig(AuthType type) {
        this.type = type;
    }

    protected AuthProviderConfig(AuthType type, boolean enabled) {
        this(type);
        this.enabled = enabled;
    }

    abstract public <T> T accept(AuthConfigVisitor<T> visitor);
}
