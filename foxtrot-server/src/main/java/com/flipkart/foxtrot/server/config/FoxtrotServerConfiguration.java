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
package com.flipkart.foxtrot.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseConfig;
import com.flipkart.foxtrot.core.querystore.impl.ClusterConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import io.dropwizard.Configuration;
import net.sourceforge.cobertura.CoverageIgnore;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:26 PM
 */
@CoverageIgnore
public class FoxtrotServerConfiguration extends Configuration {
    @Valid
    private HbaseConfig hbase;

    @Valid
    private ElasticsearchConfig elasticsearch;

    @Valid
    private ClusterConfig cluster;

    @Valid
    @JsonProperty("deletionconfig")
    private DataDeletionManagerConfig deletionManagerConfig;

    public HbaseConfig getHbase() {
        return hbase;
    }

    public void setHbase(HbaseConfig hbase) {
        this.hbase = hbase;
    }

    public ElasticsearchConfig getElasticsearch() {
        return elasticsearch;
    }

    public void setElasticsearch(ElasticsearchConfig elasticsearch) {
        this.elasticsearch = elasticsearch;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public void setCluster(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public DataDeletionManagerConfig getDeletionManagerConfig() {
        return deletionManagerConfig;
    }

    public void setDeletionManagerConfig(DataDeletionManagerConfig deletionManagerConfig) {
        this.deletionManagerConfig = deletionManagerConfig;
    }
}
