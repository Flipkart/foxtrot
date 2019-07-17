package com.flipkart.foxtrot.core.alerts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.internalevents.InternalEventBusConsumer;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEventVisitor;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessed;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import javax.inject.Inject;
import java.util.Map;

/**
 * Listens to {@link InternalSystemEvent} and sends alert emails if necessary
 */
@Slf4j
public class AlertingSystemEventConsumer implements InternalEventBusConsumer {

    private final EmailClient emailClient;
    private final ObjectMapper objectMapper;

    @Inject
    public AlertingSystemEventConsumer(
            EmailClient emailClient,
            ObjectMapper objectMapper) {
        this.emailClient = emailClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public void process(InternalSystemEvent event) {
        final AlertEmail email = event.accept(new EmailBuilder());
        if (null == email) {
            return;
        }
        emailClient.sendEmail(email);
    }

    private class EmailBuilder implements InternalSystemEventVisitor<AlertEmail> {

        @Override
        public AlertEmail visit(QueryProcessed queryProcessed) {
            return null;
        }

        @Override
        public AlertEmail visit(QueryProcessingError queryProcessingError) {
            final FoxtrotException exception = queryProcessingError.getException();
            switch (exception.getCode()) {
                case CARDINALITY_OVERFLOW: {
                    final Map<String, Object> context = exception.toMap();
                    break;
                }
                case TABLE_INITIALIZATION_ERROR:
                case TABLE_METADATA_FETCH_FAILURE:
                case TABLE_NOT_FOUND:
                case TABLE_ALREADY_EXISTS:
                case TABLE_MAP_STORE_ERROR:
                case INVALID_REQUEST:
                case DOCUMENT_NOT_FOUND:
                case MALFORMED_QUERY:
                case ACTION_RESOLUTION_FAILURE:
                case UNRESOLVABLE_OPERATION:
                case ACTION_EXECUTION_ERROR:
                case STORE_CONNECTION_ERROR:
                case STORE_EXECUTION_ERROR:
                case DATA_CLEANUP_ERROR:
                case EXECUTION_EXCEPTION:
                case CONSOLE_SAVE_EXCEPTION:
                case CONSOLE_FETCH_EXCEPTION:
                case AUTHORIZATION_EXCEPTION:
                case SOURCE_MAP_CONVERSION_FAILURE:
                case FQL_PERSISTENCE_EXCEPTION:
                case FQL_PARSE_ERROR:
                case PORT_EXTRACTION_ERROR:
                    break;
            }
            return null;
        }

        private void sendEmail(GroupRequest parameter, String email, double probability) {
            try {
                String subject = "Blocked query as it might have screwed up the cluster";
                String content = objectMapper.writeValueAsString(parameter);
                String recipients = "";
                if (!StringUtils.isEmpty(email)) {
                    recipients = recipients + ", " + email;
                }
                emailClient.sendEmail(AlertEmail.builder()
                                              .content(content)
                                              .subject(subject)
                                              .recipients(recipients)
                                              .build());
                log.warn("Blocked query as it might have screwed up the cluster. Probability: {} Query: {}",
                         probability, content);
            }
            catch (JsonProcessingException e) {
                log.warn("Blocked query as it might have screwed up the cluster. Probability: {} Query: {}",
                         probability,
                         parameter);
            }
        }
    }
}
