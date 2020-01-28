package com.flipkart.foxtrot.core.alerts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.email.Email;
import com.flipkart.foxtrot.core.email.RichEmailBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailBodyBuilder;
import com.flipkart.foxtrot.core.email.messageformatting.impl.StrSubstitutorEmailSubjectBuilder;
import com.flipkart.foxtrot.core.exception.CardinalityOverflowException;
import com.flipkart.foxtrot.core.internalevents.events.QueryProcessingError;
import com.google.common.collect.Lists;
import io.dropwizard.jackson.Jackson;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class EmailBuilderTest {

    @Test
    public void testCardinalityEmailBuild() throws JsonProcessingException {
        EmailBuilder emailBuilder = new EmailBuilder(new RichEmailBuilder(new StrSubstitutorEmailSubjectBuilder(),
                                                                          new StrSubstitutorEmailBodyBuilder()));
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Lists.newArrayList("os", "deviceId"));

        QueryProcessingError error = new QueryProcessingError(
                groupRequest,
                new CardinalityOverflowException(groupRequest,
                                                 Jackson.newObjectMapper()
                                                         .writeValueAsString(
                                                                 groupRequest),
                                                 "deviceId",
                                                 0.75));
        final Email email = emailBuilder.visit(error);
        Assert.assertEquals("Blocked query as it might have screwed up the cluster", email.getSubject());
        Assert.assertEquals(
                "Blocked Query: {\"opcode\":\"group\",\"filters\":[],\"table\":\"test-table\"," +
                        "\"uniqueCountOn\":null,\"nesting\":[\"os\",\"deviceId\"]}\n" +
                        "Suspect field: deviceId\n" +
                        "Probability of screwing up the cluster: 0.75",
                email.getContent());
    }

}