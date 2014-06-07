/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 3:46 PM
 */
public class ElasticsearchUtils {
    public static final String TYPE_NAME = "document";
    public static final String TABLENAME_PREFIX = "foxtrot";
    public static final String TABLENAME_POSTFIX = "table";
    private static ObjectMapper mapper;

    public static void setMapper(ObjectMapper mapper) {
        ElasticsearchUtils.mapper = mapper;
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static String[] getIndices(final String table) {
        /*long currentTime = new Date().getTime();
        String names[] = new String[30]; //TODO::USE TABLE METADATA
        for(int i = 0 ; i < 30; i++) {
            String postfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(currentTime));
            names[i] = String.format("%s-%s-%s", TABLENAME_PREFIX, table, postfix);
        }*/
        return new String[]{String.format("%s-%s-%s-*",
                ElasticsearchUtils.TABLENAME_PREFIX, table, ElasticsearchUtils.TABLENAME_POSTFIX)};
    }

    public static String getCurrentIndex(final String table, long timestamp) {
        //TODO::THROW IF TIMESTAMP IS BEYOND TABLE META.TTL
        String datePostfix = new SimpleDateFormat("dd-M-yyyy").format(new Date(timestamp));
        return String.format("%s-%s-%s-%s", ElasticsearchUtils.TABLENAME_PREFIX, table,
                ElasticsearchUtils.TABLENAME_POSTFIX, datePostfix);
    }

    public static PutIndexTemplateRequest getClusterTemplateMapping(IndicesAdminClient indicesAdminClient) {
        PutIndexTemplateRequestBuilder builder = new PutIndexTemplateRequestBuilder(indicesAdminClient, "generic_template");
        builder.setTemplate("foxtrot-*");
        builder.addMapping(TYPE_NAME, "{\n" +
                "        \"dynamic_templates\" : [ {\n" +
                "          \"template_timestamp\" : {\n" +
                "            \"mapping\" : {\n" +
                "              \"index\" : \"not_analyzed\",\n" +
                "              \"store\" : false,\n" +
                "              \"type\" : \"date\"\n" +
                "            },\n" +
                "            \"match\" : \"timestamp\"\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"template_no_store\" : {\n" +
                "            \"mapping\" : {\n" +
                "              \"store\" : false\n" +
                "            },\n" +
                "            \"match\" : \"*\"\n" +
                "          }\n" +
                "        } ],\n" +
                "        \"_all\" : {\n" +
                "          \"enabled\" : false\n" +
                "        },\n" +
                "        \"_timestamp\" : {\n" +
                "          \"enabled\" : true\n," +
                "          \"store\" : true\n" +
                "        },\n" +
                "        \"_source\" : {\n" +
                "          \"enabled\" : false\n" +
                "        }}");
        return builder.request();
    }

    public static void initializeMappings(Client client) {
        PutIndexTemplateRequest templateRequest = getClusterTemplateMapping(client.admin().indices());
        client.admin().indices().putTemplate(templateRequest).actionGet();
    }

    public static String getValidTableName(String table) {
        if (table == null) return null;
        return table.toLowerCase();
    }
}
