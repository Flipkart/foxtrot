package com.flipkart.foxtrot.core.nodegroup;

import com.flipkart.foxtrot.common.exception.NodeGroupExecutionException;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.core.nodegroup.visitors.NodeAllocationTableIndexPatternVisitor;
import com.flipkart.foxtrot.core.nodegroup.visitors.NodeAllocationTemplateOrderVisitor;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Set;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getNodeAllocationTemplateName;

@Slf4j
@Singleton
public class AllocationManagerImpl implements AllocationManager {

    private static final String INDEX_ROUTING_ALLOCATION_INCLUDE_NAME = "index.routing.allocation.include._name";
    private static final String INDEX_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE = "index.routing.allocation.total_shards_per_node";
    private final ElasticsearchConnection connection;

    @Inject
    public AllocationManagerImpl(final ElasticsearchConnection connection) {
        this.connection = connection;
    }

    @Override
    public void createNodeAllocationTemplate(AllocatedESNodeGroup esNodeGroup) {
        try {
            PutIndexTemplateRequest templateRequest = buildNodeAllocationTemplateRequest(esNodeGroup);
            connection.getClient()
                    .indices()
                    .putTemplate(templateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error occurred while saving node allocation template for node group : {}", esNodeGroup, e);
            throw new NodeGroupExecutionException("Error saving node allocation template for node group", e);
        }
    }

    @Override
    public void deleteNodeAllocationTemplate(String groupName) {
        try {

            DeleteIndexTemplateRequest deleteIndexTemplateRequest = new DeleteIndexTemplateRequest();
            deleteIndexTemplateRequest.name(getNodeAllocationTemplateName(groupName));
            connection.getClient()
                    .indices()
                    .deleteTemplate(deleteIndexTemplateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("Error occurred while deleting node allocation template for node group : {}", groupName, e);
            throw new NodeGroupExecutionException(
                    "Error deleting node allocation template for node group: " + groupName, e);
        }
    }

    @Override
    public void syncAllocationSettings(Set<String> indices,
                                       AllocatedESNodeGroup esNodeGroup) {
        indices.forEach(index -> {
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(
                    getIndexAllocationSettings(esNodeGroup));
            try {
                AcknowledgedResponse updateSettingsResponse = connection.getClient()
                        .indices()
                        .putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
                if (!updateSettingsResponse.isAcknowledged()) {
                    throw new NodeGroupExecutionException(
                            "Unable to update allocation settings for index: " + index + " Reason: unacknowledged");
                }
            } catch (IOException e) {
                throw new NodeGroupExecutionException("Unable to update allocation settings for index: " + index, e);
            }

        });
    }


    private PutIndexTemplateRequest buildNodeAllocationTemplateRequest(final AllocatedESNodeGroup esNodeGroup) {
        return new PutIndexTemplateRequest().name(getNodeAllocationTemplateName(esNodeGroup.getGroupName()))
                .patterns(esNodeGroup.getTableAllocation()
                        .accept(new NodeAllocationTableIndexPatternVisitor()))
                .order(esNodeGroup.getTableAllocation()
                        .accept(new NodeAllocationTemplateOrderVisitor()))
                .settings(getIndexAllocationSettings(esNodeGroup));

    }

    private Settings getIndexAllocationSettings(AllocatedESNodeGroup esNodeGroup) {
        return Settings.builder()
                .put(INDEX_ROUTING_ALLOCATION_INCLUDE_NAME, String.join(",", esNodeGroup.getNodePatterns()))
                .put(INDEX_ROUTING_ALLOCATION_TOTAL_SHARDS_PER_NODE, esNodeGroup.getTableAllocation()
                        .getTotalShardsPerNode())
                .build();
    }
}
