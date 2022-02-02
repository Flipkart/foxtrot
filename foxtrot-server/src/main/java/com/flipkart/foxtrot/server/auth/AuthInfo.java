package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.AuthInfoType;
import lombok.Data;

/**
 *
 */
@Data
public abstract class AuthInfo {
    private final AuthInfoType type;

    protected AuthInfo(AuthInfoType type) {
        this.type = type;
    }

    public abstract <T> T accept(final AuthInfoVisitor<T> visitor);
}
