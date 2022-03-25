package com.flipkart.foxtrot.pipeline.resolver;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.io.Serializable;
import java.net.URI;

@Data
@AllArgsConstructor
@Builder
@Slf4j
public class HttpConfiguration implements Serializable {

    private String host;

    private int port;

    private String environment;

    @Builder.Default
    private String serviceName = "foxtrot";

    @Builder.Default
    private boolean secure = false;

    @Builder.Default
    private boolean usingZookeeper = true;


    private String namespace;

    @Min(1)
    @Max(1024)
    @Builder.Default
    private int connections = 10;

    @Max(86400)
    @Min(1)
    @Builder.Default
    private int idleTimeOutSeconds = 30;

    @Max(86400)
    @Min(1)
    @Builder.Default
    private int connectTimeoutMs = 10000;

    @Max(86400)
    @Min(1)
    @Builder.Default
    private int opTimeoutMs = 10000;

    @Min(0)
    @Builder.Default
    private int retryInterval = 1000;
    @Builder.Default
    private String basePath = "";

    public HttpConfiguration() {
        this.serviceName = "foxtrot";
        this.secure = false;
        this.usingZookeeper = true;
        this.connections = 10;
        this.idleTimeOutSeconds = 30;
        this.connectTimeoutMs = 10000;
        this.opTimeoutMs = 10000;
        this.retryInterval = 1000;
        this.basePath = "";
    }

    @ValidationMethod(message = "not a valid endpoint")
    @JsonIgnore
    public boolean isValidEndpoint() {
        if (Strings.isNullOrEmpty(host)) {
            return false;
        }
        try {
            URI.create(getUrl());
            return true;
        } catch (Exception e) {
            log.error("Error creating URI", e);
            return false;
        }
    }

    @ValidationMethod(message = "not a valid atlas client config")
    @JsonIgnore
    public boolean isValidZkDiscoveryConfig() {
        if (usingZookeeper) {
            return !StringUtils.isEmpty(environment) && !StringUtils.isEmpty(namespace)
                    && !StringUtils.isEmpty(serviceName);
        }
        return !StringUtils.isEmpty(host);
    }

    @JsonIgnore
    public String getUrl() {
        return String.format("%s://%s:%d", secure
                ? "https"
                : "http", host, port);
    }
}

