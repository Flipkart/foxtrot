package com.flipkart.foxtrot.server.auth;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 *
 */
@Data
@NoArgsConstructor
public class JwtConfig {
    @NotNull
    @NotEmpty
    private String privateKey;

    @NotNull
    @NotEmpty
    private String issuerId;

    @NotEmpty
    @NotNull
    private String authCachePolicy = "maximumSize=10000, expireAfterAccess=10m";

    @VisibleForTesting
    public JwtConfig(String privateKey, String issuerId) {
        this.privateKey = privateKey;
        this.issuerId = issuerId;
    }
}
