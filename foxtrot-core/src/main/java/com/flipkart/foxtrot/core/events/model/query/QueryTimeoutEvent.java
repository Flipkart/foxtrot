package com.flipkart.foxtrot.core.events.model.query;

import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.core.events.model.Event;
import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

@Data
@Builder
public class QueryTimeoutEvent implements TrackingEvent<QueryTimeoutEvent> {

    private String actionRequest;

    private String opCode;

    private String cacheKey;

    private Map<String, String> requestTags;

    private SourceType sourceType;

    private String consoleId;

    private String table;

    @Override
    public Event<QueryTimeoutEvent> toIngestionEvent() {
        return Event.<QueryTimeoutEvent>builder().groupingKey(cacheKey)
                .eventType("QUERY_TIMEOUT")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }
}
