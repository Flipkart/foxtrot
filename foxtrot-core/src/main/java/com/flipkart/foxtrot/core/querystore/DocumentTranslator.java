package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.IdentityKeyDistributor;
import com.flipkart.foxtrot.core.querystore.actions.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.sematext.hbase.ds.AbstractRowKeyDistributor;
import com.sematext.hbase.ds.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by santanu.s on 24/11/15.
 */
public class DocumentTranslator {

    private static final Logger logger = LoggerFactory.getLogger(DocumentTranslator.class);

    private String rawKeyVersion;
    private final AbstractRowKeyDistributor keyDistributor;

    public DocumentTranslator(HbaseConfig hbaseConfig) {
        if (CollectionUtils.isNullOrEmpty(hbaseConfig.getRawKeyVersion())
                || hbaseConfig.getRawKeyVersion().equalsIgnoreCase("1.0")) {
            this.keyDistributor = new IdentityKeyDistributor();
            this.rawKeyVersion = "1.0";
        } else if (hbaseConfig.getRawKeyVersion().equalsIgnoreCase("2.0")) {
            this.keyDistributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(32));
            this.rawKeyVersion = "2.0";
        } else {
            throw new IllegalArgumentException(String.format("rawKeyVersion not supported version=[%s]", hbaseConfig.getRawKeyVersion()));
        }
    }

    public List<Document> translate(final Table table, final List<Document> inDocuments) {
        ImmutableList.Builder<Document> docListBuilder = ImmutableList.builder();
        for (Document document : inDocuments) {
            docListBuilder.add(translate(table, document));
        }
        return docListBuilder.build();
    }

    public Document translate(final Table table, final Document inDocument) {
        Document document = new Document();
        DocumentMetadata metadata = metadata(table, inDocument);

        switch (rawKeyVersion) {
            case "1.0":
                document.setId(inDocument.getId());
                break;
            case "2.0":
                document.setId(metadata.getRawStorageId());
                break;
            default:
                throw new IllegalArgumentException(String.format("rawKeyVersion not supported version=[%s]", rawKeyVersion));
        }
        document.setTimestamp(inDocument.getTimestamp());
        document.setMetadata(metadata);
        document.setData(inDocument.getData());

        return document;
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

        //logger.debug("Doc row key: {}, {}, {}", rowKey, inDocument, metadata);
        return metadata;
    }

    public String rawStorageIdFromDocument(final Table table, final Document document) {
        switch (rawKeyVersion) {
            case "1.0":
                return document.getId() + ":" + table.getName();
            case "2.0":
                return String.format("%s:%020d:%s:%s",
                        table.getName(), document.getTimestamp(), document.getId(), Constants.rawKeyVersionToSuffixMap.get(rawKeyVersion));
            default:
                throw new IllegalArgumentException(String.format("rawKeyVersion not supported version=[%s]", rawKeyVersion));
        }
    }

    @VisibleForTesting
    public String generateScalableKey(String id) {
        return new String(keyDistributor.getDistributedKey(Bytes.toBytes(id)));
    }

    public String rawStorageIdFromDocumentId(Table table, String id) {
        if (id.endsWith(Constants.rawKeyVersionToSuffixMap.get("2.0"))) {
            return id;
        }

        return String.format("%s:%s", id, table.getName());
        //IMPLEMENTOR NOTE:: Handle older versions here
    }
}
