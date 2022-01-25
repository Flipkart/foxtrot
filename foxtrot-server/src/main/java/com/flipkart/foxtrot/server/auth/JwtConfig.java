package com.flipkart.foxtrot.server.auth;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.Duration;
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
    private String privateKey = "useless_default_key";

    @NotNull
    @NotEmpty
    private String issuerId = "foxtrot";

    @NotEmpty
    @NotNull
    private String authCachePolicy = "maximumSize=10000, expireAfterAccess=10m";

    private Duration sessionDuration = Duration.days(30);

    @VisibleForTesting
    public JwtConfig(String privateKey, String issuerId) {
        this.privateKey = privateKey;
        this.issuerId = issuerId;
    }
}
