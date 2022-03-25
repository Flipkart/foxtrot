package com.flipkart.foxtrot.common.enums;

import com.google.common.base.Strings;

import java.util.Map;
import java.util.Objects;

public enum SourceType {

    FQL {
        @Override
        public boolean validate(Map<String, String> requestTags) {
            return true;
        }
    },
    ECHO_DASHBOARD {
        @Override
        public boolean validate(Map<String, String> requestTags) {
            return Objects.nonNull(requestTags) && !Strings.isNullOrEmpty(requestTags.get(WIDGET_NAME))
                    && !Strings.isNullOrEmpty(requestTags.get(CONSOLE_ID));
        }
    },
    ECHO_BROWSE_EVENTS {
        @Override
        public boolean validate(Map<String, String> requestTags) {
            return true;
        }
    },
    SERVICE {
        @Override
        public boolean validate(Map<String, String> requestTags) {
            return Objects.nonNull(requestTags) && !Strings.isNullOrEmpty(requestTags.get(SERVICE_NAME));
        }
    },
    SCRIPT {
        @Override
        public boolean validate(Map<String, String> requestTags) {
            return Objects.nonNull(requestTags) && !Strings.isNullOrEmpty(requestTags.get(SCRIPT_NAME));
        }
    };

    public static final String SERVICE_NAME = "serviceName";
    public static final String SCRIPT_NAME = "scriptName";
    private static final String WIDGET_NAME = "widget";
    private static final String CONSOLE_ID = "consoleId";

    public abstract boolean validate(Map<String, String> requestTags);

}
