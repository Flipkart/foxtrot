package com.flipkart.foxtrot.server.auth;

import com.flipkart.foxtrot.server.AuthInfoType;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

/**
 *
 */
@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TokenAuthInfo extends AuthInfo {

    String token;

    public TokenAuthInfo(String token) {
        super(AuthInfoType.TOKEN);
        this.token = token;
    }

    @Override
    public <T> T accept(AuthInfoVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
