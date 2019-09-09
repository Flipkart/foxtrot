package com.flipkart.foxtrot.core.alerts;

import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEvent;
import com.flipkart.foxtrot.core.internalevents.InternalSystemEventVisitor;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessed;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;
import com.google.common.base.CaseFormat;

import java.util.Collections;

/**
 *
 */
class EmailBuilder implements InternalSystemEventVisitor<Email> {

    private final RichEmailBuilder richEmailBuilder;

    EmailBuilder(RichEmailBuilder richEmailBuilder) {
        this.richEmailBuilder = richEmailBuilder;
    }


    @Override
    public Email visit(QueryProcessed queryProcessed) {
        return null;
    }

    @Override
    public Email visit(QueryProcessingError queryProcessingError) {
        final FoxtrotException exception = queryProcessingError.getException();
        switch (exception.getCode()) {
            case CARDINALITY_OVERFLOW: {
                return richEmailBuilder.build(templateIdFromEvent(queryProcessingError),
                                                                          Collections.emptyList(),
                                                                          exception.toMap());
            }
            default:
                break;
        }
        return null;
    }

    private String templateIdFromEvent(InternalSystemEvent event) {
        return event.accept(new InternalSystemEventVisitor<String>() {
            @Override
            public String visit(QueryProcessed queryProcessed) {
                return nameFromEventType();
            }

            @Override
            public String visit(QueryProcessingError queryProcessingError) {
                return nameFromEventType() + "_" + queryProcessingError.getException()
                        .getCode()
                        .name()
                        .toLowerCase();
            }

            private String nameFromEventType() {
                return CaseFormat.UPPER_CAMEL
                        .converterTo(CaseFormat.LOWER_UNDERSCORE)
                        .convert(event.getClass().getSimpleName());
            }
        });
    }
}
