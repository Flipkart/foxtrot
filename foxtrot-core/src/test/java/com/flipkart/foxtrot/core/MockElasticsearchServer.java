package com.flipkart.foxtrot.core;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by rishabh.goyal on 16/04/14.
 */

public class MockElasticsearchServer {
    private String DATA_DIRECTORY = "target/elasticsearch-data";
    private final Node node;

    public MockElasticsearchServer(String directory) {
        this.DATA_DIRECTORY = "target/" + directory;
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("path.data", "target/" + DATA_DIRECTORY);

        node = nodeBuilder()
                .local(true)
                .settings(elasticsearchSettings.build())
                .node();
    }

    public Client getClient() {
        return node.client();
    }

    public void shutdown() throws IOException {
        node.close();
        deleteDataDirectory();
    }

    private void deleteDataDirectory() throws IOException {
        FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    }
}