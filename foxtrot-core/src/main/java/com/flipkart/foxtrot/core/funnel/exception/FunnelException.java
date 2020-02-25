package com.flipkart.foxtrot.core.funnel.exception;

import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Getter
public class FunnelException extends FoxtrotException {

    private String funnelId;
    private String documentId;
    private String funnelName;

    private FunnelException(ErrorCode errorCode) {
        super(errorCode);
    }

    private FunnelException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    private FunnelException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    private FunnelException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }


    public static class FunnelExceptionBuilder {

        private FunnelException funnelException;

        private FunnelExceptionBuilder(FunnelException funnelException) {
            this.funnelException = funnelException;
        }

        public static FunnelExceptionBuilder builder(ErrorCode errorCode) {
            return new FunnelExceptionBuilder(new FunnelException(errorCode));
        }

        public static FunnelExceptionBuilder builder(ErrorCode errorCode, String message) {
            return new FunnelExceptionBuilder(new FunnelException(errorCode, message));
        }

        public static FunnelExceptionBuilder builder(ErrorCode errorCode, Throwable cause) {
            return new FunnelExceptionBuilder(new FunnelException(errorCode, cause));
        }

        public static FunnelExceptionBuilder builder(ErrorCode errorCode, String message, Throwable cause) {
            return new FunnelExceptionBuilder(new FunnelException(errorCode, message, cause));
        }

        public FunnelExceptionBuilder funnelId(String funnelId) {
            this.funnelException.funnelId = funnelId;
            return this;
        }

        public FunnelExceptionBuilder documentId(String documentId) {
            this.funnelException.documentId = documentId;
            return this;
        }


        public FunnelExceptionBuilder funnelName(String funnelName) {
            this.funnelException.funnelName = funnelName;
            return this;
        }

        public FunnelException build() {
            return this.funnelException;
        }

    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("funnelId", this.funnelId);
        map.put("documentId", this.documentId);
        map.put("funnelName", this.funnelName);
        map.put("message", this.getCause() != null ? this.getCause().getMessage() : this.getMessage());
        return map;
    }
}
