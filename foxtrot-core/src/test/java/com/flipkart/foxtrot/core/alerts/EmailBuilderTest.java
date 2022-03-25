package com.flipkart.foxtrot.core.alerts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.cardinality.ProbabilityCalculationResult;
import com.flipkart.foxtrot.common.enums.SourceType;
import com.flipkart.foxtrot.common.exception.CardinalityOverflowException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.dropwizard.jackson.Jackson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class EmailBuilderTest {

    @Test
    public void testCardinalityEmailBuild() throws JsonProcessingException {
        SerDe.init(new ObjectMapper());
        EmailConfig emailConfig = new EmailConfig();
        emailConfig.setBlockedQueryEmailEnabled(true);
        EmailBuilder emailBuilder = new EmailBuilder(
                new RichEmailBuilder(new StrSubstitutorEmailSubjectBuilder(), new StrSubstitutorEmailBodyBuilder()));
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Lists.newArrayList("os", "deviceId"));
        groupRequest.setConsoleId("bad console");
        groupRequest.setSourceType(SourceType.ECHO_DASHBOARD);

        Map<String, Long> groupingColumnCardinality = Maps.newHashMap();
        groupingColumnCardinality.put("deviceId", 100L);
        QueryProcessingError error = new QueryProcessingError(groupRequest,
                new CardinalityOverflowException(groupRequest, Jackson.newObjectMapper()
                        .writeValueAsString(groupRequest), Collections.singletonList("deviceId"),
                        groupRequest.getConsoleId(), "cacheKey", ProbabilityCalculationResult.builder()
                        .estimatedMaxDocCount(100000)
                        .estimatedDocCountBasedOnTime(5000)
                        .estimatedDocCountAfterFilters(1000)
                        .groupingColumnCardinality(groupingColumnCardinality)
                        .outputCardinality(500)
                        .maxCardinality(100)
                        .probability(0.75)
                        .build()));

        final Email email = emailBuilder.visit(error);
        Assert.assertEquals("Blocked query as it might have screwed up the cluster", email.getSubject());
    }

}