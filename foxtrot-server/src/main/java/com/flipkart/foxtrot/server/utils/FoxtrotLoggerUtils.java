package com.flipkart.foxtrot.server.utils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.spi.FilterAttachable;
import com.flipkart.foxtrot.server.config.LogRotationFileConfig;
import com.google.common.base.Optional;
import com.yammer.dropwizard.config.LoggingConfiguration;
import com.yammer.dropwizard.logging.LogFormatter;

/**
 * Created by anzer.moid on 10/02/18.
 */
public class FoxtrotLoggerUtils {
    public static FileAppender<ILoggingEvent> buildFileAppender(LoggingConfiguration.FileConfiguration file,
                                                                LoggerContext context,
                                                                Optional<String> logFormat,
                                                                LogRotationFileConfig logRotationConfig) {
        final LogFormatter formatter = new LogFormatter(context, file.getTimeZone());
        for (String format : logFormat.asSet()) {
            formatter.setPattern(format);
        }
        formatter.start();

        final FileAppender<ILoggingEvent> appender =
                file.isArchive() ? new RollingFileAppender<ILoggingEvent>() :
                        new FileAppender<ILoggingEvent>();

        appender.setAppend(true);
        appender.setContext(context);
        appender.setLayout(formatter);
        appender.setFile(file.getCurrentLogFilename());
        appender.setPrudent(false);

        addThresholdFilter(appender, file.getThreshold());

        if (file.isArchive()) {

            SizeBasedTriggeringPolicy triggeringPolicy = new SizeBasedTriggeringPolicy(logRotationConfig.getMaxFileSize());

            final FixedWindowRollingPolicy windowRollingPolicy = new FixedWindowRollingPolicy();
            windowRollingPolicy.setMinIndex(logRotationConfig.getMinIndex());
            windowRollingPolicy.setMaxIndex(logRotationConfig.getMaxIndex());
            windowRollingPolicy.setFileNamePattern(logRotationConfig.getFilePattern());
            windowRollingPolicy.setContext(context);

            ((RollingFileAppender<ILoggingEvent>) appender).setRollingPolicy(windowRollingPolicy);
            ((RollingFileAppender<ILoggingEvent>) appender).setTriggeringPolicy(triggeringPolicy);

            windowRollingPolicy.setParent(appender);
            windowRollingPolicy.start();
        }

        appender.stop();
        appender.start();

        return appender;
    }

    private static void addThresholdFilter(FilterAttachable<ILoggingEvent> appender, Level threshold) {
        final ThresholdFilter filter = new ThresholdFilter();
        filter.setLevel(threshold.toString());
        filter.start();
        appender.addFilter(filter);
    }
}
