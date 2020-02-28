package com.flipkart.foxtrot.core.funnel.services;

import static com.collections.CollectionUtils.nullSafeList;
import static com.flipkart.foxtrot.core.exception.ErrorCode.EXECUTION_EXCEPTION;

import com.collections.CollectionUtils;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.constants.FunnelConstants;
import com.flipkart.foxtrot.core.funnel.exception.FunnelException;
import com.flipkart.foxtrot.core.funnel.model.EventAttributes;
import com.flipkart.foxtrot.core.funnel.model.Funnel;
import com.flipkart.foxtrot.core.funnel.model.FunnelData;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV2;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelEventResponseV1;
import com.flipkart.foxtrot.core.funnel.model.FunnelInfo;
import com.flipkart.foxtrot.core.funnel.model.request.EventProcessingRequest;
import com.flipkart.foxtrot.core.funnel.model.response.EventProcessingResponseV1;
import com.flipkart.foxtrot.core.funnel.model.response.FunnelEventResponseV2;
import com.flipkart.foxtrot.core.funnel.persistence.FunnelStore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 25/09/18
 ***/
@Singleton
public class EventProcessingServiceImpl implements EventProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessingServiceImpl.class);

    private final FunnelStore funnelStore;
    private final BaseFunnelEventConfig baseFunnelEventConfig;

    @Inject
    public EventProcessingServiceImpl(final FunnelStore funnelStore,
            final BaseFunnelEventConfig baseFunnelEventConfig) {
        this.funnelStore = funnelStore;
        this.baseFunnelEventConfig = baseFunnelEventConfig;
    }

    @Override
    public EventProcessingResponseV2 process(EventProcessingRequest eventProcessingRequest) throws FoxtrotException {
        List<Funnel> funnels = processRequest(eventProcessingRequest);
        EventProcessingResponseV2 eventProcessingResponse = parseFunnels(funnels);
        //if hash code matches then only sending hash value in response to save network
        if (eventProcessingResponse.getResponseHashCode() == eventProcessingRequest.getResponseHashCode()) {
            return new EventProcessingResponseV2(eventProcessingRequest.getResponseHashCode());
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
            throw new FunnelException(EXECUTION_EXCEPTION, "Device Id is missing");
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

    private EventProcessingResponseV2 parseFunnels(List<Funnel> funnels) {
        Map<Tuple<String, String>, List<FunnelInfo>> eventCategoryVsFunnelIds = Maps.newHashMap();
        for (Funnel funnel : CollectionUtils.nullSafeList(funnels)) {
            List<EventAttributes> eventAttributes = funnel.getEventAttributes();
            for (EventAttributes attributes : nullSafeList(eventAttributes)) {
                Tuple<String, String> eventCategory = new Tuple<>(attributes.getEventType(),
                        attributes.getCategory());
                FunnelInfo funnelInfo = FunnelInfo.builder()
                        .funnelId(funnel.getId())
                        .funnelData(FunnelData.builder()
                                .startPercentage(funnel.getStartPercentage())
                                .endPercentage(funnel.getEndPercentage())
                                .build())
                        .build();
                if (eventCategoryVsFunnelIds.containsKey(eventCategory)) {
                    eventCategoryVsFunnelIds.get(eventCategory)
                            .add(funnelInfo);
                } else {
                    eventCategoryVsFunnelIds.put(eventCategory, Lists.newArrayList(funnelInfo));
                }
            }
        }
        List<FunnelEventResponseV2> funnelEventResponses = Lists.newArrayList();
        for (Map.Entry<Tuple<String, String>, List<FunnelInfo>> entry : eventCategoryVsFunnelIds.entrySet()) {
            FunnelEventResponseV2 funnelEventResponse = new FunnelEventResponseV2();
            funnelEventResponse.setEventType(entry.getKey().v1());
            funnelEventResponse.setCategory(entry.getKey().v2());

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
                    FunnelEventResponseV2 funnelEventResponse = new FunnelEventResponseV2();
                    funnelEventResponse.setEventType(baseFunnelEventConfig.getEventType());
                    funnelEventResponse.setCategory(baseFunnelEventConfig.getCategory());
                    funnelEventResponse.setFunnelInfos(Lists.newArrayList(funnelInfo));
                    funnelEventResponses.add(funnelEventResponse);
                }
            }
        } catch (FoxtrotException e) {
            LOGGER.error("Error retrieving latest funnel!!!");
        }

        if (CollectionUtils.isEmpty(funnelEventResponses)) {
            return new EventProcessingResponseV2();
        }
        return new EventProcessingResponseV2(funnelEventResponses);
    }

    private boolean setBaseEventInfo(List<FunnelEventResponseV2> funnelEventResponsV1s, FunnelInfo funnelInfo) {
        for (FunnelEventResponseV2 funnelEventResponse : nullSafeList(funnelEventResponsV1s)) {
            if (baseFunnelEventConfig.getEventType().equals(funnelEventResponse.getEventType())) {
                funnelEventResponse.setFunnelInfos(Lists.newArrayList(funnelInfo));
                return true;
            }
        }
        return false;
    }
}
