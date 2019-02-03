package com.flipkart.foxtrot.core.querystore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.querystore.actions.Constants;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

public class DocumentTranslatorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRawKeyVersion() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setRawKeyVersion(UUID.randomUUID().toString());
        new DocumentTranslator(hbaseConfig);
    }

    @Test
    public void testTranslationWithNullRawKeyVersion() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setRawKeyVersion(null);
        DocumentTranslator translator = new DocumentTranslator(hbaseConfig);
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());

        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode().put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        assertEquals(translatedDocument.getId(), document.getId());
        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getMetadata().getId(), document.getId());
        assertEquals(translatedDocument.getMetadata().getRawStorageId(), document.getId() + ":" + table.getName());
    }

    @Test
    public void testTranslationWithRawKeyVersion1() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1());
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());

        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode().put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        assertEquals(translatedDocument.getId(), document.getId());
        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getMetadata().getId(), document.getId());
        assertEquals(translatedDocument.getMetadata().getRawStorageId(), document.getId() + ":" + table.getName());
    }

    @Test
    public void testTranslationWithRawKeyVersion2() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV2());
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());

        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode().put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        assertNotNull(translatedDocument.getMetadata());
        assertEquals(translatedDocument.getId(), translatedDocument.getMetadata().getRawStorageId());
        assertEquals(translatedDocument.getMetadata().getId(), document.getId());
        assertTrue(translatedDocument.getMetadata().getRawStorageId().endsWith(Constants.rawKeyVersionToSuffixMap.get("2.0")));
    }

    @Test
    public void testTranslationBackWithRawKeyVersion1() {
        DocumentTranslator translator = new DocumentTranslator(TestUtils.createHBaseConfigWithRawKeyV1());
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());

        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode().put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        Document translatedBackDocument = translator.translateBack(translatedDocument);

        assertEquals(document.getId(), translatedBackDocument.getId());
        assertEquals(document.getTimestamp(), translatedBackDocument.getTimestamp());
    }

    @Test
    public void testTranslationBackWithRawKeyVersion2() {
        HbaseConfig hbaseConfig = new HbaseConfig();
        hbaseConfig.setRawKeyVersion("2.0");

        DocumentTranslator translator = new DocumentTranslator(hbaseConfig);
        Table table = new Table();
        table.setName(UUID.randomUUID().toString());

        Document document = new Document();
        document.setId(UUID.randomUUID().toString());
        document.setTimestamp(System.currentTimeMillis());
        document.setData(mapper.createObjectNode().put("name", "rishabh"));

        Document translatedDocument = translator.translate(table, document);

        Document translatedBackDocument = translator.translateBack(translatedDocument);

        assertEquals(document.getId(), translatedBackDocument.getId());
        assertEquals(document.getTimestamp(), translatedBackDocument.getTimestamp());
    }

}