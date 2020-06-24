package com.foxtrot.flipkart.translator;

import static com.collections.CollectionUtils.nullSafeList;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.DocumentMetadata;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.exception.SerDeException;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.util.Utils;
import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.foxtrot.flipkart.translator.config.UnmarshallerConfig;
import com.foxtrot.flipkart.translator.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.sematext.hbase.ds.AbstractRowKeyDistributor;
import com.sematext.hbase.ds.RowKeyDistributorByHashPrefix;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by santanu.s on 24/11/15.
 */
@Slf4j
@Singleton
public class DocumentTranslator {

    private static final String EXCEPTION_MESSAGE = "rawKeyVersion not supported version=[%s]";
    private static final String JSON_PATH_SEPARATOR = "/";
    private final AbstractRowKeyDistributor keyDistributor;
    private final String rawKeyVersion;
    private final UnmarshallerConfig unmarshallerConfig;

    @Inject
    public DocumentTranslator(TranslatorConfig translatorConfig) {
        if (CollectionUtils.isNullOrEmpty(translatorConfig.getRawKeyVersion()) || translatorConfig.getRawKeyVersion()
                .equalsIgnoreCase("1.0")) {
            this.keyDistributor = new IdentityKeyDistributor();
            this.rawKeyVersion = "1.0";
        } else if (translatorConfig.getRawKeyVersion()
                .equalsIgnoreCase("2.0")) {
            this.keyDistributor = new RowKeyDistributorByHashPrefix(
                    new RowKeyDistributorByHashPrefix.OneByteSimpleHash(32));
            this.rawKeyVersion = "2.0";
        } else if (translatorConfig.getRawKeyVersion()
                .equalsIgnoreCase("3.0")) {
            this.keyDistributor = new RowKeyDistributorByHashPrefix(
                    new RowKeyDistributorByHashPrefix.OneByteSimpleHash(256));
            this.rawKeyVersion = "3.0";
        } else {
            throw new IllegalArgumentException(String.format(EXCEPTION_MESSAGE, translatorConfig.getRawKeyVersion()));
        }
        this.unmarshallerConfig = translatorConfig.getUnmarshallerConfig();
    }

    public List<Document> translate(final Table table,
                                    final List<Document> inDocuments) {
        ImmutableList.Builder<Document> docListBuilder = ImmutableList.builder();
        for (Document document : inDocuments) {
            docListBuilder.add(translate(table, document));
        }
        return docListBuilder.build();
    }

    public Document translate(final Table table,
                              final Document inDocument) {
        Document document = new Document();
        DocumentMetadata metadata = metadata(table, inDocument);

        switch (rawKeyVersion) {
            case "1.0":
                document.setId(inDocument.getId());
                break;
            case "2.0":
            case "3.0":
                document.setId(metadata.getRawStorageId());
                break;
            default:
                throw new IllegalArgumentException(String.format(EXCEPTION_MESSAGE, rawKeyVersion));
        }

        ObjectNode dataNode = inDocument.getData()
                .deepCopy();

        if (unmarshallerConfig.isUnmarshallingEnabled() && unmarshallerConfig.getTableVsUnmarshallJsonPath()
                .containsKey(table.getName())) {
            List<String> unmarshallJsonPaths = unmarshallerConfig.getTableVsUnmarshallJsonPath()
                    .get(table.getName());
            unmarshallStringJsonFields(dataNode, unmarshallJsonPaths);
        }

        document.setTimestamp(inDocument.getTimestamp());
        document.setMetadata(metadata);
        document.setData(dataNode);
        document.setDate(Utils.getDate(inDocument.getTimestamp()));

        return document;
    }

    private void unmarshallStringJsonFields(ObjectNode dataNode,
                                            List<String> unmarshallJsonPaths) {
        for (String jsonPath : nullSafeList(unmarshallJsonPaths)) {
            try {
                JsonPointer valueNodePointer = JsonPointer.compile(jsonPath);
                JsonPointer containerPointer = valueNodePointer.head();
                JsonNode parentJsonNode = dataNode.at(containerPointer);

                if (!parentJsonNode.isMissingNode() && parentJsonNode.isObject()) {
                    ObjectNode parentObjectNode = (ObjectNode) parentJsonNode;
                    //following will give you just the field name.
                    //e.g. if pointer is /parentObject/object/field
                    //JsonPointer.last() will give you /field
                    //remember to take out the / character
                    String fieldName = valueNodePointer.last()
                            .toString();
                    fieldName = fieldName.replace(JSON_PATH_SEPARATOR, StringUtils.EMPTY);
                    JsonNode fieldValueNode = parentObjectNode.get(fieldName);

                    if (fieldValueNode != null && fieldValueNode.isTextual()) {
                        JsonNode jsonNode = JsonUtils.toJsonNode(fieldValueNode.asText());
                        parentObjectNode.set(fieldName, jsonNode);
                    }
                }
            } catch (SerDeException e) {
                if(log.isDebugEnabled()){
                    log.debug("Error while expanding field at json path : {}", jsonPath, e);
                }
                throw e;
            } catch (Exception e) {
                if(log.isDebugEnabled()) {
                    log.debug("Error while expanding field at json path : {}, eating exception ", jsonPath, e);
                }
            }
        }
    }

    public Document translateBack(final Document inDocument) {
        Document document = new Document();
        document.setId(inDocument.getMetadata() != null
                       ? inDocument.getMetadata()
                               .getId()
                       : inDocument.getId());
        document.setTimestamp(inDocument.getTimestamp());
        document.setData(inDocument.getData());
        document.setDate(Utils.getDate(inDocument.getTimestamp()));
        return document;
    }

    public DocumentMetadata metadata(final Table table,
                                     final Document inDocument) {
        final String rowKey = generateScalableKey(rawStorageIdFromDocument(table, inDocument));
        DocumentMetadata metadata = new DocumentMetadata();
        metadata.setRawStorageId(rowKey);
        metadata.setId(inDocument.getId());
        metadata.setTime(inDocument.getTimestamp());
        return metadata;
    }


    public String rawStorageIdFromDocument(final Table table,
                                           final Document document) {
        switch (rawKeyVersion) {
            case "1.0":
                return document.getId() + ":" + table.getName();
            case "2.0":
            case "3.0":
                return String.format("%s:%020d:%s:%s", table.getName(), document.getTimestamp(), document.getId(),
                        Constants.RAW_KEY_VERSION_TO_SUFFIX_MAP.get(rawKeyVersion));
            default:
                throw new IllegalArgumentException(String.format(EXCEPTION_MESSAGE, rawKeyVersion));
        }
    }

    @VisibleForTesting
    public String generateScalableKey(String id) {
        return new String(keyDistributor.getDistributedKey(Bytes.toBytes(id)));
    }

    public String rawStorageIdFromDocumentId(Table table,
                                             String id) {
        if (id.endsWith(Constants.RAW_KEY_VERSION_TO_SUFFIX_MAP.get("2.0")) || id.endsWith(
                Constants.RAW_KEY_VERSION_TO_SUFFIX_MAP.get("3.0"))) {
            return id;
        }

        return String.format("%s:%s", id, table.getName());
        //IMPLEMENTOR NOTE:: Handle older versions here
    }
}
