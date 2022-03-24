/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com) Date: 24/03/14 Time: 3:46 PM
 */
public class ElasticsearchUtils {

    public static final String DOCUMENT_TYPE_NAME = "document";
    public static final String TENANT_TYPE_NAME = "tenant";
    public static final String DOCUMENT_META_FIELD_NAME = "__FOXTROT_METADATA__";
    public static final String DOCUMENT_META_TIMESTAMP_FIELD_NAME = String.format("%s.time", DOCUMENT_META_FIELD_NAME);
    public static final String TIME_FIELD = "time";
    public static final String TYPE = "type";
    public static final int DEFAULT_SUB_LIST_SIZE = 50;
    public static final int DEFAULT_REFRESH_INTERVAL_IN_SECS = 30;
    public static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("dd-M-yyyy");
    static final String DOCUMENT_META_ID_FIELD_NAME = String.format("%s.id", DOCUMENT_META_FIELD_NAME);
    static final String DOCUMENT_TIME_FIELD_NAME = "date";
    private static final String PROPERTIES = "properties";
    private static final String STORE = "store";
    private static final String TABLENAME_POSTFIX = "table";
    private static final String DEFAULT_NAME_PREFIX = "foxtrot";
    private static final String TENANT_NAME_POSTFIX = "tenant";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("dd-M-yyyy");
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchUtils.class.getSimpleName());
    private static final String PATH_MATCH = "path_match";
    private static final String MAPPING = "mapping";
    private static final String INDEX = "index";
    private static final String MATCH_MAPPING_TYPE = "match_mapping_type";
    private static String namePrefix = DEFAULT_NAME_PREFIX;

    private ElasticsearchUtils() {
    }

    @VisibleForTesting
    public static String getNamePrefix() {
        return namePrefix;
    }

    public static void setNamePrefix(ElasticsearchConfig config) {
        if (StringUtils.isNotEmpty(config.getTableNamePrefix())) {
            namePrefix = config.getTableNamePrefix();
        }
    }

    public static String getTableIndexPattern() {
        return getNamePrefix() + "-*-" + TABLENAME_POSTFIX + "-*";
    }

    public static String getMappingTemplateName(final String table) {
        return String.format("template_foxtrot_%s_mappings", table);
    }

    public static String getNodeAllocationTemplateName(final String groupName) {
        return String.format("template_foxtrot_%s_node_allocation", groupName);
    }

    public static String getIndexPrefix(final String table) {
        return String.format("%s-%s-%s-", getNamePrefix(), table, ElasticsearchUtils.TABLENAME_POSTFIX);
    }

    public static String getIndices(final String table) {
        return String.format("%s-%s-%s-*", getNamePrefix(), table, ElasticsearchUtils.TABLENAME_POSTFIX);
    }

    public static String[] getIndices(final String table,
                                      final ActionRequest request) {
        return getIndices(table, request, new PeriodSelector(request.getFilters()).analyze());
    }

    @VisibleForTesting
    public static String[] getIndices(final String table,
                                      final ActionRequest request,
                                      final Interval interval) {
        DateTime start = interval.getStart()
                .toLocalDate()
                .toDateTimeAtStartOfDay();
        if (start.getYear() <= 1970) {
            logger.warn("Request of type {} running on all indices", request.getClass()
                    .getSimpleName());
            return new String[]{getIndices(table)};
        }
        List<String> indices = Lists.newArrayList();
        final DateTime end = interval.getEnd()
                .plusDays(1)
                .toLocalDate()
                .toDateTimeAtStartOfDay();
        while (start.getMillis() < end.getMillis()) {
            final String index = getCurrentIndex(table, start.getMillis());
            indices.add(index);
            start = start.plusDays(1);
        }
        logger.info("Request of type {} on indices: {}", request.getClass()
                .getSimpleName(), indices);
        return indices.toArray(new String[0]);
    }

    public static String getCurrentIndex(final String table,
                                         long timestamp) {
        //TODO::THROW IF TIMESTAMP IS BEYOND TABLE META.TTL
        String datePostfix = FORMATTER.print(timestamp);
        return String.format("%s-%s-%s-%s", getNamePrefix(), table, ElasticsearchUtils.TABLENAME_POSTFIX, datePostfix);
    }

    public static String getTenantCurrentIndex(final String tenant) {
        return String.format("%s-%s-%s", getNamePrefix(), tenant, ElasticsearchUtils.TENANT_NAME_POSTFIX);
    }

    public static PutIndexTemplateRequest getClusterTemplateMapping() {
        try {
            return new PutIndexTemplateRequest().name("template_foxtrot_mappings")
                    .patterns(Lists.newArrayList(String.format("%s-*", getNamePrefix())))
                    .mapping(DOCUMENT_TYPE_NAME, getDocumentMapping());
        } catch (IOException ex) {
            logger.error("TEMPLATE_CREATION_FAILED", ex);
            return null;
        }
    }

    private static XContentBuilder getDocumentMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .field(DOCUMENT_TYPE_NAME)
                .startObject()
                .field("_source")
                .startObject()
                .field("enabled", false)
                .endObject()
                .field("dynamic_templates")
                .startArray()

                .startObject()
                .field("template_metadata_timestamp")
                .startObject()
                .field(PATH_MATCH, ElasticsearchUtils.DOCUMENT_META_TIMESTAMP_FIELD_NAME)
                .field(MAPPING)
                .startObject()
                .field(STORE, true)
                .field(INDEX, true)
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()

                .startObject()
                .field("template_metadata_string")
                .startObject()
                .field(PATH_MATCH, ElasticsearchUtils.DOCUMENT_META_FIELD_NAME + ".*")
                .field(MATCH_MAPPING_TYPE, "string")
                .field(MAPPING)
                .startObject()
                .field(STORE, true)
                .field(INDEX, true)
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()

                .startObject()
                .field("template_metadata_others")
                .startObject()
                .field(PATH_MATCH, ElasticsearchUtils.DOCUMENT_META_FIELD_NAME + ".*")
                .field(MAPPING)
                .startObject()
                .field(STORE, true)
                .field(INDEX, true)
                .endObject()
                .endObject()
                .endObject()

                .startObject()
                .field("template_no_store_analyzed")
                .startObject()
                .field("match", "*")
                .field(MATCH_MAPPING_TYPE, "string")
                .field(MAPPING)
                .startObject()
                .field(STORE, true)
                .field(INDEX, true)
                .field("type", "keyword")
                .field("fields")
                .startObject()
                .field("analyzed")
                .startObject()
                .field(STORE, false)
                .field(INDEX, true)
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()

                .startObject()
                .field("template_no_store")
                .startObject()
                .field(MATCH_MAPPING_TYPE, "*")
                .field("match_pattern", "regex")
                .field(PATH_MATCH, ".*")
                .field(MAPPING)
                .startObject()
                .field(STORE, false)
                .field(INDEX, true)
                .endObject()
                .endObject()
                .endObject()

                .endArray()
                .field(PROPERTIES)
                .startObject()
                .field("eventData")
                .startObject()
                .field(PROPERTIES)
                .startObject()
                .field("funnelInfo")
                .startObject()
                .field(PROPERTIES)
                .startObject()
                .field("funnelId")
                .startObject()
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()

                .field("time")
                .startObject()
                .field("type", "long")
                .field("fields")
                .startObject()
                .field("date")
                .startObject()
                .field(INDEX, "true")
                .field(STORE, true)
                .field("type", "date")
                .field("format", "epoch_millis")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    public static void initializeMappings(RestHighLevelClient client) {
        PutIndexTemplateRequest templateRequest = getClusterTemplateMapping();
        try {
            client.indices()
                    .putTemplate(templateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error occurred: ", e);
        }
    }


    public static String getValidName(String name) {
        if (name == null) {
            return null;
        }
        return name.trim()
                .toLowerCase();
    }

    private static boolean isIndexValidForTable(String index,
                                                String table) {
        String indexPrefix = getIndexPrefix(table);
        return index.startsWith(indexPrefix);
    }

    static boolean isIndexEligibleForDeletion(String index,
                                              Table table) {
        if (index == null || table == null || !isIndexValidForTable(index, table.getName())) {
            return false;
        }

        DateTime creationDate = parseIndexDate(index, table.getName());
        DateTime startTime = new DateTime(0L);
        DateTime endTime = new DateTime().minusDays(table.getTtl())
                .toDateTime();
        return creationDate.isAfter(startTime) && creationDate.isBefore(endTime);
    }

    public static DateTime parseIndexDate(String index,
                                          String table) {
        return DATE_TIME_FORMATTER.parseDateTime(getIndexDatePostfix(index, table));
    }

    public static DateTime parseIndexDate(String index) {
        String table = getTableNameFromIndex(index);
        return parseIndexDate(index, table);
    }

    public static String getIndexDatePostfix(String index,
                                             String table) {
        String indexPrefix = getIndexPrefix(table);
        return index.substring(index.indexOf(indexPrefix) + indexPrefix.length());
    }

    public static String getIndexDatePostfix(String index) {
        String table = getTableNameFromIndex(index);
        return getIndexDatePostfix(index, table);
    }

    public static String getTableNameFromIndex(String currentIndex) {
        if (currentIndex.contains(getNamePrefix()) && currentIndex.contains(TABLENAME_POSTFIX)) {
            String tempIndex = currentIndex.substring(
                    currentIndex.indexOf(getNamePrefix()) + getNamePrefix().length() + 1);
            int position = tempIndex.lastIndexOf(String.format("-%s", TABLENAME_POSTFIX));
            return tempIndex.substring(0, position);
        } else {
            return null;
        }
    }

    static String getAllIndicesPattern() {
        return String.format("%s-*-%s-*", getNamePrefix(), ElasticsearchUtils.TABLENAME_POSTFIX);
    }

    public static String getTodayIndicesPattern() {
        String datePostfix = FORMATTER.print(LocalDate.now());
        return String.format("%s-.*-%s-%s", getNamePrefix(), ElasticsearchUtils.TABLENAME_POSTFIX, datePostfix);
    }

    public static boolean isValidDatePostFix(String dateStr) {
        try {
            LocalDate.parse(dateStr, FORMATTER);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    public static String getIndicesPatternForDate(String datePostFix) {
        return String.format("%s-.*-%s-%s", getNamePrefix(), ElasticsearchUtils.TABLENAME_POSTFIX, datePostFix);
    }

    public static boolean isTimeFilterPresent(List<Filter> filters) {
        AtomicBoolean timeFilterPresent = new AtomicBoolean(false);
        filters.forEach(filter -> {
            if (ElasticsearchUtils.TIME_FIELD.equals(filter.getField())) {
                timeFilterPresent.set(true);
            }
        });
        return timeFilterPresent.get();
    }
}
