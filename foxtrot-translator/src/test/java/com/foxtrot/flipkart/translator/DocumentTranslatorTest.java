package com.foxtrot.flipkart.translator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Date;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.util.SerDe;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.foxtrot.flipkart.translator.config.UnmarshallerConfig;
import com.foxtrot.flipkart.translator.utils.Constants;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@Slf4j
public class DocumentTranslatorTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String NAME = "rishabh";

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRawKeyVersion() {
        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion(UUID.randomUUID()
                .toString());
        new DocumentTranslator(translatorConfig);
    }

    @Test
    public void testTranslationWithNullRawKeyVersion() {
        TranslatorConfig hbaseConfig = new TranslatorConfig();
        hbaseConfig.setRawKeyVersion(null);
        DocumentTranslator translator = new DocumentTranslator(hbaseConfig);
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", NAME));

        Document translatedDocument = translator.translate(table, document);

        assertEquals(translatedDocument.getId(), document.getId());
        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getMetadata()
                .getId(), document.getId());
        assertEquals(translatedDocument.getMetadata()
                .getRawStorageId(), document.getId() + ":" + table.getName());
    }

    @Test
    public void testTranslationWithRawKeyVersion1() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createTranslatorConfigWithRawKeyV1());
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", NAME));

        Document translatedDocument = translator.translate(table, document);

        assertEquals(translatedDocument.getId(), document.getId());
        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getMetadata()
                .getId(), document.getId());
        assertEquals(translatedDocument.getMetadata()
                .getRawStorageId(), document.getId() + ":" + table.getName());
    }

    @Test
    public void testTranslationWithRawKeyVersion2() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createTranslatorConfigWithRawKeyV2());
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", NAME));

        Document translatedDocument = translator.translate(table, document);

        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getId(), translatedDocument.getMetadata()
                .getRawStorageId());
        assertEquals(translatedDocument.getMetadata()
                .getId(), document.getId());
        assertTrue(translatedDocument.getMetadata()
                .getRawStorageId()
                .endsWith(Constants.RAW_KEY_VERSION_TO_SUFFIX_MAP.get("2.0")));
    }

    @Test
    public void testTranslationWithRawKeyVersion3() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createTranslatorConfigWithRawKeyV3());
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", "nitish"));

        Document translatedDocument = translator.translate(table, document);

        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getId(), translatedDocument.getMetadata()
                .getRawStorageId());
        assertEquals(translatedDocument.getMetadata()
                .getId(), document.getId());
        assertTrue(translatedDocument.getMetadata()
                .getRawStorageId()
                .endsWith(Constants.RAW_KEY_VERSION_TO_SUFFIX_MAP.get("3.0")));
    }

    @Test
    public void testTranslationBackWithRawKeyVersion1() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createTranslatorConfigWithRawKeyV1());
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", NAME));

        Document translatedDocument = translator.translate(table, document);

        Document translatedBackDocument = translator.translateBack(translatedDocument);

        assertEquals(document.getId(), translatedBackDocument.getId());
        assertEquals(document.getTimestamp(), translatedBackDocument.getTimestamp());
    }

    @Test
    public void testTranslationBackWithRawKeyVersion2() {
        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion("2.0");

        DocumentTranslator translator = new DocumentTranslator(translatorConfig);
        Table table = new Table();
        table.setName(UUID.randomUUID()
                .toString());

        Document document = new Document();
        document.setId(UUID.randomUUID()
                .toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode()
                .put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        Document translatedBackDocument = translator.translateBack(translatedDocument);

        assertEquals(document.getId(), translatedBackDocument.getId());
        assertEquals(document.getTimestamp(), translatedBackDocument.getTimestamp());
    }

    @Test
    public void testJsonStringFieldExpansionToJsonNode() throws IOException {
        SerDe.init(mapper);

        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion("2.0");
        translatorConfig.setUnmarshallerConfig(UnmarshallerConfig.builder()
                .unmarshallingEnabled(true)
                .tableVsUnmarshallJsonPath(ImmutableMap.of("phonepe_consumer_app_android_new", Arrays.asList(
                        new String[]{"/eventData/funnelInfo", "/eventData/funnelInfoData/funnelInfos/funnelIds"})))
                .build());

        DocumentTranslator documentTranslator = new DocumentTranslator(translatorConfig);

        Table table = Table.builder()
                .name("phonepe_consumer_app_android_new")
                .build();

        Document document = new Document();
        document.setDate(new Date());
        document.setId(UUID.randomUUID()
                .toString());
        String eventJson = "{\n" + "  \"app\": \"phonepe_consumer_app_android_new\",\n" + "  \"eventData\": {\n"
                + "    \"value\": 0,\n" + "    \"category\": \"SYNC_MANAGER\",\n"
                + "    \"flowType_id\": \"c42e1be6-b72e-4d40-91f7-cd31b7f34ccb\",\n"
                + "    \"mobileDataType\": \"4G\",\n" + "    \"eventId\": \"SYNC_MANAGER_SYSTEM_REGISTRATION\",\n"
                + "    \"appVersion\": \"4.0.07\",\n" + "    \"isFirstTimeRegistration\": false,\n"
                + "    \"deviceResolution\": \"720X1436\",\n" + "    \"latitude\": \"17.995245\",\n"
                + "    \"currentNetwork\": \"NO_NETWORK\",\n" + "    \"osName\": \"Android\",\n"
                + "    \"deviceId\": \"28b61b61-77ea-4e09-93de-f2e0d3a3b12aazYydjFfNjRfYnNw-bXQ2NzYy-\",\n"
                + "    \"userId\": \"U1809251957496281968828\",\n" + "    \"versionCode\": 400302,\n"
                + "    \"funnelInfo\": \"[{\\\"funnelData\\\":{\\\"startPercentage\\\":0.0,\\\"endPercentage\\\":4.0},\\\"funnelId\\\":\\\"72bce962-058f-458f-ad22-dfb3ba40aaf4\\\"}]\",\n"
                + "    \"identifierId\": \"SYNC_MANAGER\",\n" + "    \"isConfigurationChanged\": false,\n"
                + "    \"osVersion\": \"27\",\n" + "    \"flowType_medium\": \"Marketing\",\n"
                + "    \"flowType_campaign\": \"200405_OTHER_DONATION_1\",\n" + "    \"deviceLanguage\": \"English\",\n"
                + "    \"deviceModel\": \"vivo 1803\",\n" + "    \"flowType_source\": \"Push\",\n"
                + "    \"deviceManufacturer\": \"vivo\",\n" + "    \"flowType\": \"push\",\n"
                + "    \"longitude\": \"78.74735333333334\"\n" + "  },\n" + "  \"eventSchemaVersion\": \"v2\",\n"
                + "  \"eventType\": \"SYNC_MANAGER_SYSTEM_REGISTRATION\",\n"
                + "  \"groupingKey\": \"b354287cead141b1b40d0567a418c7d7\",\n"
                + "  \"id\": \"b354287cead141b1b40d0567a418c7d7\",\n" + "  \"ingestionTime\": 1586736296057,\n"
                + "  \"partitionKey\": null,\n" + "  \"time\": 1586721949229\n" + "}";
        document.setData(mapper.readTree(eventJson));
        Document translatedDocument = documentTranslator.translate(table, document);
        log.info("Translated document :{}", translatedDocument);
        Assert.assertTrue(translatedDocument.getData()
                .at("/eventData/funnelInfo")
                .isArray());
        eventJson = "{\n" + "  \"app\": \"phonepe_consumer_app_android_new\",\n" + "  \"eventData\": {\n"
                + "    \"value\": 0,\n" + "    \"category\": \"SYNC_MANAGER\",\n"
                + "    \"flowType_id\": \"c42e1be6-b72e-4d40-91f7-cd31b7f34ccb\",\n"
                + "    \"mobileDataType\": \"4G\",\n" + "    \"eventId\": \"SYNC_MANAGER_SYSTEM_REGISTRATION\",\n"
                + "    \"appVersion\": \"4.0.07\",\n" + "    \"isFirstTimeRegistration\": false,\n"
                + "    \"deviceResolution\": \"720X1436\",\n" + "    \"latitude\": \"17.995245\",\n"
                + "    \"currentNetwork\": \"NO_NETWORK\",\n" + "    \"osName\": \"Android\",\n"
                + "    \"deviceId\": \"28b61b61-77ea-4e09-93de-f2e0d3a3b12aazYydjFfNjRfYnNw-bXQ2NzYy-\",\n"
                + "    \"userId\": \"U1809251957496281968828\",\n" + "    \"versionCode\": 400302,\n"
                + "    \"funnelInfoData\": {\n" + "      \"funnelInfos\": {\n"
                + "        \"funnelIds\": \"[{\\\"funnelData\\\":{\\\"startPercentage\\\":0.0,\\\"endPercentage\\\":4.0},\\\"funnelId\\\":\\\"72bce962-058f-458f-ad22-dfb3ba40aaf4\\\"}]\"\n"
                + "      }\n" + "    },\n" + "    \"identifierId\": \"SYNC_MANAGER\",\n"
                + "    \"isConfigurationChanged\": false,\n" + "    \"osVersion\": \"27\",\n"
                + "    \"flowType_medium\": \"Marketing\",\n"
                + "    \"flowType_campaign\": \"200405_OTHER_DONATION_1\",\n" + "    \"deviceLanguage\": \"English\",\n"
                + "    \"deviceModel\": \"vivo 1803\",\n" + "    \"flowType_source\": \"Push\",\n"
                + "    \"deviceManufacturer\": \"vivo\",\n" + "    \"flowType\": \"push\",\n"
                + "    \"longitude\": \"78.74735333333334\"\n" + "  },\n" + "  \"eventSchemaVersion\": \"v2\",\n"
                + "  \"eventType\": \"SYNC_MANAGER_SYSTEM_REGISTRATION\",\n"
                + "  \"groupingKey\": \"b354287cead141b1b40d0567a418c7d7\",\n"
                + "  \"id\": \"b354287cead141b1b40d0567a418c7d7\",\n" + "  \"ingestionTime\": 1586736296057,\n"
                + "  \"partitionKey\": null,\n" + "  \"time\": 1586721949229\n" + "}";
        document.setData(mapper.readTree(eventJson));

        translatedDocument = documentTranslator.translate(table, document);
        log.info("Translated document :{}", translatedDocument);
        Assert.assertTrue(translatedDocument.getData()
                .at("/eventData/funnelInfoData/funnelInfos/funnelIds")
                .isArray());

    }
}
