package com.flipkart.foxtrot.core.alerts;

import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.EmailClient;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;

/**
 * Listens to {@link InternalSystemEvent} and sends alert emails if necessary
 */
@Slf4j
public class AlertingSystemEventConsumer implements InternalEventBusConsumer {

    private final EmailClient emailClient;
    private final RichEmailBuilder richEmailBuilder;

    @Inject
    public AlertingSystemEventConsumer(
            EmailClient emailClient,
            RichEmailBuilder richEmailBuilder) {
        this.emailClient = emailClient;
        this.richEmailBuilder = richEmailBuilder;
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
