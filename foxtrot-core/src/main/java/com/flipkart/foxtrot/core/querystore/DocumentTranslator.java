package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by santanu.s on 24/11/15.
 */
public class DocumentTranslator {
    private static final Logger logger = LoggerFactory.getLogger(DocumentTranslator.class);

    public List<Document> translate(final Table table, final List<Document> inDocuments) {
        ImmutableList.Builder<Document> docListBuilder = ImmutableList.builder();
        for(Document document : inDocuments) {
            docListBuilder.add(translate(table, document));
        }
        return docListBuilder.build();
    }

    public Document translate(final Table table, final Document inDocument) {
        Document document = new Document();
        final String rowKey = rowKey(table, inDocument);

        DocumentMetadata metadata = new DocumentMetadata();
        metadata.setRowKey(rowKey);
        metadata.setId(inDocument.getId());

        document.setId(rowKey);
        document.setTimestamp(inDocument.getTimestamp());
        document.setMetadata(metadata);
        document.setData(inDocument.getData());

        logger.info("Translated doc row key: {}, {}", rowKey, document);
        return document;
    }

    public List<Document> translateBack(final List<Document> inDocuments) {
        ImmutableList.Builder<Document> listBuilder = ImmutableList.builder();
        if(null != inDocuments) {
            for (Document document : inDocuments) {
                listBuilder.add(translateBack(document));
            }
        }
        return listBuilder.build();
    }

    public Document translateBack(final Document inDocument) {
        Document document = new Document();
        document.setId(inDocument.getMetadata() != null ? inDocument.getMetadata().getId() : inDocument.getId());
        document.setTimestamp(inDocument.getTimestamp());
        document.setData(inDocument.getData());
        return document;
    }

    public DocumentMetadata metadata(final Table table, final Document inDocument) {
        final String rowKey = rowKey(table, inDocument);

        DocumentMetadata metadata = new DocumentMetadata();
        metadata.setRowKey(rowKey);
        metadata.setId(inDocument.getId());

        logger.info("Doc row key: {}, {}", rowKey, inDocument);
        return metadata;
    }

    public String rowKey(final Table table, final Document document) {
        return String.format("%s:%d:%s", table.getName(), document.getTimestamp(), document.getId());
    }
}
