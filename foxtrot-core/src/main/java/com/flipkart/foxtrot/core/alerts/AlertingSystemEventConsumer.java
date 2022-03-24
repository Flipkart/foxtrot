package com.flipkart.foxtrot.core.alerts;

import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Listens to {@link InternalSystemEvent} and sends alert emails if necessary
 */
@Slf4j
@Singleton
public class AlertingSystemEventConsumer implements InternalEventBusConsumer {

    private final EmailClient emailClient;
    private final RichEmailBuilder richEmailBuilder;
    private final EmailConfig emailConfig;

    @Inject
    public AlertingSystemEventConsumer(EmailClient emailClient,
                                       RichEmailBuilder richEmailBuilder,
                                       EmailConfig emailConfig) {
        this.emailClient = emailClient;
        this.richEmailBuilder = richEmailBuilder;
        this.emailConfig = emailConfig;
    }

    @Override
    public void process(InternalSystemEvent event) {
        final Email email = event.accept(new EmailBuilder(richEmailBuilder));
        if (null == email) {
            return;
        }
        emailClient.sendEmail(email);
    }

}
