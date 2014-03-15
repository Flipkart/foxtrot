package com.flipkart.foxtrot.server.config;

import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.yammer.dropwizard.config.Configuration;

import javax.validation.Valid;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:26 PM
 */
public class FoxtrotServerConfiguration extends Configuration {
    @Valid
    private HbaseConfig hbase;

    @Valid
    private ElasticsearchConfig elasticsearch;

    public FoxtrotServerConfiguration() {
        this.hbase = new HbaseConfig();
        this.elasticsearch = new ElasticsearchConfig();
    }

    public HbaseConfig getHbase() {
        return hbase;
    }

    public ElasticsearchConfig getElasticsearch() {
        return elasticsearch;
    }
}
