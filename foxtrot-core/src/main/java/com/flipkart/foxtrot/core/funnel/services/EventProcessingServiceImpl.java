package com.flipkart.foxtrot.core.funnel.services;

import static com.collections.CollectionUtils.nullSafeList;
import static com.collections.CollectionUtils.nullSafeMap;
import static com.flipkart.foxtrot.core.exception.ErrorCode.EXECUTION_EXCEPTION;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.DELETED;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.END_PERCENTAGE;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.FIELD_VS_VALUES;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.FUNNEL_STATUS;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelAttributes.START_PERCENTAGE;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.DOT;
import static com.flipkart.foxtrot.core.funnel.constants.FunnelConstants.FUNNEL_INDEX;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.config.BaseEventConfig;
import com.flipkart.foxtrot.core.funnel.constants.FunnelConstants;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException.FunnelExceptionBuilder;
import com.flipkart.foxtrot.core.funnel.model.EventAttributes;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.FunnelData;
import com.flipkart.foxtrot.core.funnel.model.FunnelEventResponse;
import com.flipkart.foxtrot.core.funnel.model.FunnelInfo;
import com.flipkart.foxtrot.core.funnel.model.enums.FunnelStatus;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponse;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Singleton
public class EventProcessingServiceImpl implements EventProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingServiceImpl.class);

    private final FunnelStore funnelStore;
    private final BaseEventConfig baseEventConfig;

    @Inject
    public EventProcessingServiceImpl(final FunnelStore funnelStore,
            final BaseEventConfig baseEventConfig) {
        this.funnelStore = funnelStore;
        this.baseEventConfig = baseEventConfig;
    }

    @Override
    public EventProcessingResponse process(EventProcessingRequest eventProcessingRequest) throws FoxtrotException {
        List<Funnel> funnels = processRequest(eventProcessingRequest);
        EventProcessingResponse eventProcessingResponse = parseFunnels(funnels);
        //if hash code matches then only sending hash value in response to save network
        if (eventProcessingResponse.getResponseHashCode() == eventProcessingRequest.getResponseHashCode()) {
            return new EventProcessingResponse(eventProcessingRequest.getResponseHashCode());
        }
        return eventProcessingResponse;
    }

    private List<Funnel> processRequest(EventProcessingRequest eventProcessingRequest) throws FoxtrotException {
        int bucket = calculateBucket(eventProcessingRequest);

        List<Map<String, String>> fieldVsValueMaps = Arrays.asList(eventProcessingRequest.getAppSpecificFields(),
                eventProcessingRequest.getDeviceSpecificFields(),
                eventProcessingRequest.getUserSpecificFields());

        List<Funnel> funnels = funnelStore.fetchFunnels(fieldVsValueMaps, bucket);

        List<Funnel> validFunnels = new ArrayList<>();
        for (Funnel funnel : funnels) {
            if (isFunnelValid(funnel, eventProcessingRequest)) {
                validFunnels.add(funnel);
            }
        }
        return validFunnels;

    }

    private int calculateBucket(EventProcessingRequest eventProcessingRequest) {
        String deviceId = CollectionUtils.nullSafeMap(eventProcessingRequest.getDeviceSpecificFields())
                .get(FunnelConstants.DEVICE_ID);
        if (StringUtils.isEmpty(deviceId)) {
            throw FunnelExceptionBuilder.builder(EXECUTION_EXCEPTION, "Device Id is missing").build();
        }
        int hashOfDeviceId = deviceId.hashCode();
        return Math.abs(hashOfDeviceId % 100);
    }

    // this validation checks if funnel parameters are specified in the request
    private boolean isFunnelValid(Funnel funnel, EventProcessingRequest eventProcessingRequest) {
        for (String attribute : CollectionUtils.nullSafeSet(funnel.getFieldVsValues().keySet())) {
            if (CollectionUtils.nullSafeMap(eventProcessingRequest.getUserSpecificFields()).containsKey(attribute)
                    || CollectionUtils.nullSafeMap(eventProcessingRequest.getAppSpecificFields()).containsKey(attribute)
                    || CollectionUtils.nullSafeMap(eventProcessingRequest.getDeviceSpecificFields())
                    .containsKey(attribute)) {
                continue;
            }
            return false;
        }
        return true;
    }

    private EventProcessingResponse parseFunnels(List<Funnel> funnels) {
        Map<Tuple<String, String>, List<FunnelInfo>> eventIdentifierVsFunnelIds = Maps.newHashMap();
        for (Funnel funnel : CollectionUtils.nullSafeList(funnels)) {
            List<EventAttributes> eventAttributes = funnel.getEventAttributes();
            for (EventAttributes attributes : nullSafeList(eventAttributes)) {
                Tuple<String, String> eventIdentifier = new Tuple<>(attributes.getEventId(),
                        attributes.getIdentifierId());
                FunnelInfo funnelInfo = FunnelInfo.builder()
                        .funnelId(funnel.getId())
                        .funnelData(FunnelData.builder()
                                .startPercentage(funnel.getStartPercentage())
                                .endPercentage(funnel.getEndPercentage())
                                .build())
                        .build();
                if (eventIdentifierVsFunnelIds.containsKey(eventIdentifier)) {
                    eventIdentifierVsFunnelIds.get(eventIdentifier)
                            .add(funnelInfo);
                } else {
                    eventIdentifierVsFunnelIds.put(eventIdentifier, Lists.newArrayList(funnelInfo));
                }
            }
        }
        List<FunnelEventResponse> funnelEventResponses = Lists.newArrayList();
        for (Map.Entry<Tuple<String, String>, List<FunnelInfo>> entry : eventIdentifierVsFunnelIds.entrySet()) {
            FunnelEventResponse funnelEventResponse = new FunnelEventResponse();
            funnelEventResponse.setEventId(entry.getKey()
                    .v1());
            funnelEventResponse.setIdentifierId(entry.getKey()
                    .v2());
            funnelEventResponse.setFunnelInfos(entry.getValue());
            funnelEventResponses.add(funnelEventResponse);
        }
        try {
            Funnel latestFunnel = funnelStore.getLatestFunnel();
            if (latestFunnel != null) {
                FunnelInfo funnelInfo = FunnelInfo.builder()
                        .funnelId(latestFunnel.getId())
                        .build();
                if (!setBaseEventInfo(funnelEventResponses, funnelInfo)) {
                    FunnelEventResponse funnelEventResponse = new FunnelEventResponse();
                    funnelEventResponse.setEventId(baseEventConfig.getEventId());
                    funnelEventResponse.setIdentifierId(baseEventConfig.getIdentifierId());
                    funnelEventResponse.setFunnelInfos(Lists.newArrayList(funnelInfo));
                    funnelEventResponses.add(funnelEventResponse);
                }
            }
        } catch (FoxtrotException e) {
            LOGGER.error("Error retrieving latest funnel!!!");
        }

        if (CollectionUtils.isEmpty(funnelEventResponses)) {
            return new EventProcessingResponse();
        }
        return new EventProcessingResponse(funnelEventResponses);
    }

    private boolean setBaseEventInfo(List<FunnelEventResponse> funnelEventResponses, FunnelInfo funnelInfo) {
        for (FunnelEventResponse funnelEventResponse : nullSafeList(funnelEventResponses)) {
            if (baseEventConfig.getEventId().equals(funnelEventResponse.getEventId())) {
                funnelEventResponse.setFunnelInfos(Lists.newArrayList(funnelInfo));
                return true;
            }
        }
        return false;
    }
}
