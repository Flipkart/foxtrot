package com.flipkart.foxtrot.server.auth.authprovider.impl;

import com.flipkart.foxtrot.server.auth.authprovider.AuthConfigVisitor;
import com.flipkart.foxtrot.server.auth.authprovider.AuthProviderConfig;
import com.flipkart.foxtrot.server.auth.authprovider.AuthType;
import java.net.Proxy;
import javax.validation.constraints.NotNull;
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
public class GoogleAuthProviderConfig extends AuthProviderConfig {

    @NotEmpty
    @NotNull
    private String clientId;

    @NotEmpty
    @NotNull
    private String clientSecret;

    private String loginDomain;

    @NotNull
    @NotEmpty
    private String server;

    @NotNull
    private boolean secureEndpoint;

    private Proxy.Type proxyType;

    private String proxyHost;

    private int proxyPort;

    @Builder
    public GoogleAuthProviderConfig(boolean enabled,
                                    String clientId,
                                    String clientSecret,
                                    String loginDomain,
                                    String server,
                                    boolean secureEndpoint,
                                    Proxy.Type proxyType,
                                    String proxyHost,
                                    int proxyPort) {
        super(AuthType.OAUTH_GOOGLE, enabled);
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.loginDomain = loginDomain;
        this.server = server;
        this.secureEndpoint = secureEndpoint;
        this.proxyType = proxyType;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
    }

    public GoogleAuthProviderConfig() {
        super(AuthType.OAUTH_GOOGLE);
    }

    @Override
    public <T> T accept(AuthConfigVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
