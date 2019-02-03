package com.flipkart.foxtrot.sql;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flipkart.foxtrot.sql.responseprocessors.FlatteningUtils;
import com.flipkart.foxtrot.sql.responseprocessors.model.FlatRepresentation;
import org.junit.Assert;
import org.junit.Test;

public class TestsMultiRowParse {
    @Test
    public void parseMeta() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        final String json = "{\"table\":\"europa\",\"mappings\":[{\"field\":\"data.checkoutId\",\"type\":\"STRING\"},{\"field\":\"data.errorMessage\",\"type\":\"STRING\"},{\"field\":\"data.startTime\",\"type\":\"LONG\"},{\"field\":\"data.error\",\"type\":\"STRING\"},{\"field\":\"data.pin\",\"type\":\"LONG\"},{\"field\":\"data.version\",\"type\":\"STRING\"},{\"field\":\"data.invalidationReason\",\"type\":\"STRING\"},{\"field\":\"data.request.clientHostName\",\"type\":\"STRING\"},{\"field\":\"data.storedValueType\",\"type\":\"STRING\"},{\"field\":\"data.request.clientTraceId\",\"type\":\"STRING\"},{\"field\":\"data.accountId\",\"type\":\"STRING\"},{\"field\":\"header.profile\",\"type\":\"STRING\"},{\"field\":\"data.endTime\",\"type\":\"LONG\"},{\"field\":\"data.channel.sessionId\",\"type\":\"STRING\"},{\"field\":\"data.channel.salesChannel\",\"type\":\"STRING\"},{\"field\":\"data.storedValue\",\"type\":\"STRING\"},{\"field\":\"data.duration\",\"type\":\"LONG\"},{\"field\":\"data.invalidationMessage\",\"type\":\"STRING\"},{\"field\":\"data.channel.terminalId\",\"type\":\"STRING\"},{\"field\":\"data.email\",\"type\":\"STRING\"},{\"field\":\"data.channel.channelSource\",\"type\":\"STRING\"},{\"field\":\"header.configName\",\"type\":\"STRING\"},{\"field\":\"header.appName\",\"type\":\"STRING\"},{\"field\":\"data.errorCode\",\"type\":\"STRING\"},{\"field\":\"header.timestamp\",\"type\":\"DATE\"},{\"field\":\"data.request.requestId\",\"type\":\"STRING\"},{\"field\":\"data.request.clientAppIPAddress\",\"type\":\"STRING\"},{\"field\":\"data.key\",\"type\":\"STRING\"},{\"field\":\"data.channel.createdBy\",\"type\":\"STRING\"},{\"field\":\"data.callistoAPI\",\"type\":\"STRING\"},{\"field\":\"data.addressId\",\"type\":\"STRING\"},{\"field\":\"data.channel.userAgent\",\"type\":\"STRING\"},{\"field\":\"data.salesChannel\",\"type\":\"STRING\"},{\"field\":\"header.instanceId\",\"type\":\"STRING\"},{\"field\":\"data.request.checkoutId\",\"type\":\"STRING\"},{\"field\":\"data.checkoutType\",\"type\":\"STRING\"},{\"field\":\"data.channel.userIp\",\"type\":\"STRING\"},{\"field\":\"data.tenant\",\"type\":\"STRING\"},{\"field\":\"header.eventId\",\"type\":\"STRING\"},{\"field\":\"data.actionType.type\",\"type\":\"STRING\"},{\"field\":\"data.request.client\",\"type\":\"STRING\"},{\"field\":\"data.dataFlow\",\"type\":\"STRING\"}]}";
        JsonNode root = objectMapper.readTree(json);
        FlatRepresentation representation = FlatteningUtils.genericMultiRowParse(root.get("mappings"), null, "field");
        System.out.println(writer.writeValueAsString(representation));
        Assert.assertEquals(2, representation.getHeaders().size());
    }

    @Test
    public void parseTable() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        final String json = "[{\"name\":\"60\",\"ttl\":15},{\"name\":\"aprameya\",\"ttl\":15},{\"name\":\"athena\",\"ttl\":7},{\"name\":\"callisto\",\"ttl\":30},{\"name\":\"cart-service\",\"ttl\":15},{\"name\":\"digital-ingestion-pipeline\",\"ttl\":60},{\"name\":\"ebooks-delivery\",\"ttl\":15},{\"name\":\"europa\",\"ttl\":60},{\"name\":\"fk-w3-co-prezente\",\"ttl\":15},{\"name\":\"fk-w3-uss\",\"ttl\":60},{\"name\":\"flip-sync\",\"ttl\":30},{\"name\":\"flipcast\",\"ttl\":30},{\"name\":\"flipkart-ebook\",\"ttl\":60},{\"name\":\"flipkart-ebooks\",\"ttl\":30},{\"name\":\"flipkartretailapp\",\"ttl\":30},{\"name\":\"flipkartretailapp-referral\",\"ttl\":60},{\"name\":\"ganymede\",\"ttl\":30},{\"name\":\"santa\",\"ttl\":30},{\"name\":\"selfserve\",\"ttl\":15},{\"name\":\"warehouse\",\"ttl\":30},{\"name\":\"webreader\",\"ttl\":60}]";
        JsonNode root = objectMapper.readTree(json);
        FlatRepresentation representation = FlatteningUtils.genericMultiRowParse(root, null, "name");
        System.out.println(writer.writeValueAsString(representation));
        Assert.assertEquals(2, representation.getHeaders().size());
    }
}
