package com.foxtrot.flipkart.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.foxtrot.flipkart.translator.utils.Constants;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

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

}
