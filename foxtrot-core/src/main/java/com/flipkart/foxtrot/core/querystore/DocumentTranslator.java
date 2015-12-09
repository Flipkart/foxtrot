package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.shash.hbase.ds.AbstractRowKeyDistributor;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by santanu.s on 24/11/15.
 */
public class DocumentTranslator {
    private static final String CURRENT_RAW_KEY_VERSION = "__RAW_KEY_VERSION_2__";

    private static final Logger logger = LoggerFactory.getLogger(DocumentTranslator.class);

    private final AbstractRowKeyDistributor keyDistributor;

    public DocumentTranslator(AbstractRowKeyDistributor keyDistributor) {
        this.keyDistributor = keyDistributor;
    }

    public List<Document> translate(final Table table, final List<Document> inDocuments) {
        ImmutableList.Builder<Document> docListBuilder = ImmutableList.builder();
        for(Document document : inDocuments) {
            docListBuilder.add(translate(table, document));
        }
        return docListBuilder.build();
    }

    public Document translate(final Table table, final Document inDocument) {
        Document document = new Document();
        DocumentMetadata metadata = metadata(table, inDocument);

        document.setId(metadata.getRawStorageId());
        document.setTimestamp(inDocument.getTimestamp());
        document.setMetadata(metadata);
        document.setData(inDocument.getData());

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
        final String rowKey = generateScalableKey(rawStorageIdFromDocument(table, inDocument));

        DocumentMetadata metadata = new DocumentMetadata();
        metadata.setRawStorageId(rowKey);
        metadata.setId(inDocument.getId());

        logger.debug("Doc row key: {}, {}", rowKey, inDocument);
        return metadata;
    }

    public String rawStorageIdFromDocument(final Table table, final Document document) {
        return String.format("%s:%020d:%s:%s",
                table.getName(), document.getTimestamp(), document.getId(), CURRENT_RAW_KEY_VERSION);
    }

    @VisibleForTesting
    public String generateScalableKey(String id) {
        return new String(keyDistributor.getDistributedKey(Bytes.toBytes(id)));
    }

    public String rawStorageIdFromDocumentId(Table table, String id) {
        if(!id.endsWith(CURRENT_RAW_KEY_VERSION)) {
            return String.format("%s:%s", id, table.getName());
        }
        //IMPLEMENTOR NOTE:: Handle older versions here
        return id;
    }
}
