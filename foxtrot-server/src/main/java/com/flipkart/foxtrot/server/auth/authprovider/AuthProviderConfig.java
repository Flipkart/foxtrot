package com.flipkart.foxtrot.server.auth.authprovider;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.foxtrot.server.auth.authprovider.impl.GoogleAuthProviderConfig;
import lombok.Data;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(name = "OAUTH_GOOGLE", value = GoogleAuthProviderConfig.class),})
@Data
public abstract class AuthProviderConfig {

    private final AuthType type;

    private boolean enabled;

    protected AuthProviderConfig(AuthType type) {
        this.type = type;
    }

    protected AuthProviderConfig(AuthType type,
                                 boolean enabled) {
        this(type);
        this.enabled = enabled;
    }

    public abstract <T> T accept(AuthConfigVisitor<T> visitor);
}
