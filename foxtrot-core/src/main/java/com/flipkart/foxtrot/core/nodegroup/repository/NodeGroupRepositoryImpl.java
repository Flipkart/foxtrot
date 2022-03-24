package com.flipkart.foxtrot.core.nodegroup.repository;

import com.flipkart.foxtrot.common.exception.NodeGroupStoreException;
import com.flipkart.foxtrot.common.exception.TableMapStoreException;
import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus;
import com.flipkart.foxtrot.common.nodegroup.VacantESNodeGroup;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.util.ElasticsearchQueryUtils;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.common.nodegroup.TableAllocation.TableAllocationType.COMMON;

@Slf4j
@Singleton
public class NodeGroupRepositoryImpl implements NodeGroupRepository {

    public static final String NODE_GROUP_INDEX = "node-group";
    public static final String NODE_GROUP_TYPE = "node-group";
    private static final String STATUS_ATTRIBUTE = "status";
    private static final String TABLE_ALLOCATION_TYPE = "tableAllocation.type";
    private ElasticsearchConnection elasticsearchConnection;

    @Inject
    public NodeGroupRepositoryImpl(final ElasticsearchConnection elasticsearchConnection) {
        this.elasticsearchConnection = elasticsearchConnection;
    }

    @Override
    public void save(ESNodeGroup nodeGroup) {
        try {
            Map<String, Object> sourceMap = JsonUtils.readMapFromObject(nodeGroup);
            elasticsearchConnection.getClient()
                    .index(new IndexRequest().index(NODE_GROUP_INDEX)
                            .type(NODE_GROUP_TYPE)
                            .source(sourceMap)
                            .id(nodeGroup.getGroupName())
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("Error while saving node group :{}", nodeGroup, e);
            throw new NodeGroupStoreException("Error saving node group", e);
        }
    }

    @Override
    public ESNodeGroup get(String groupName) {
        try {
            GetResponse response = elasticsearchConnection.getClient()
                    .get(new GetRequest(NODE_GROUP_INDEX, NODE_GROUP_TYPE, groupName), RequestOptions.DEFAULT);
            if (!response.isExists()) {
                return null;
            }
            return JsonUtils.fromJson(response.getSourceAsBytes(), ESNodeGroup.class);
        } catch (Exception e) {
            log.error("Error while getting node group :{}", groupName, e);
            throw new NodeGroupStoreException("Error getting node group: " + groupName, e);
        }
    }

    @Override
    public VacantESNodeGroup getVacantGroup() {
        QueryBuilder query = new MatchQueryBuilder(STATUS_ATTRIBUTE, AllocationStatus.VACANT.name());
        try {
            val searchRequest = new SearchRequest(NODE_GROUP_INDEX).types(NODE_GROUP_TYPE)
                    .source(new SearchSourceBuilder().query(query)
                            .fetchSource(true)
                            .size(1))
                    .searchType(SearchType.QUERY_THEN_FETCH);
            SearchHits response = elasticsearchConnection.getClient()
                    .search(searchRequest, RequestOptions.DEFAULT)
                    .getHits();
            if (response == null || response.getTotalHits() == 0) {
                return null;
            }
            return JsonUtils.fromJson(response.getAt(0)
                    .getSourceAsString(), VacantESNodeGroup.class);
        } catch (Exception e) {
            log.error("Error while getting vacant node group :", e);
            throw new NodeGroupStoreException("Error getting vacant node group", e);
        }

    }

    @Override
    public AllocatedESNodeGroup getCommonGroup() {
        QueryBuilder query = new MatchQueryBuilder(TABLE_ALLOCATION_TYPE, COMMON.name());
        try {
            val searchRequest = new SearchRequest(NODE_GROUP_INDEX).types(NODE_GROUP_TYPE)
                    .source(new SearchSourceBuilder().query(query)
                            .fetchSource(true)
                            .size(1))
                    .searchType(SearchType.QUERY_THEN_FETCH);
            SearchHits response = elasticsearchConnection.getClient()
                    .search(searchRequest, RequestOptions.DEFAULT)
                    .getHits();
            if (response == null || response.getTotalHits() == 0) {
                return null;
            }
            return JsonUtils.fromJson(response.getAt(0)
                    .getSourceAsString(), AllocatedESNodeGroup.class);
        } catch (Exception e) {
            log.error("Error while getting common node group :", e);
            throw new NodeGroupStoreException("Error getting common node group", e);
        }

    }

    @Override
    public List<ESNodeGroup> getAll() {
        SearchResponse response;
        try {
            response = elasticsearchConnection.getClient()
                    .search(new SearchRequest(NODE_GROUP_INDEX).types(NODE_GROUP_TYPE)
                            .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                                    .size(ElasticsearchQueryUtils.QUERY_SIZE))
                            .scroll(new TimeValue(30, TimeUnit.SECONDS)), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new NodeGroupStoreException("Error while fetching node groups: ", e);
        }
        List<ESNodeGroup> esNodeGroups = Lists.newArrayList();
        do {
            for (SearchHit hit : response.getHits()
                    .getHits()) {
                ESNodeGroup nodeGroup = JsonUtils.fromJson(hit.getSourceAsString(), ESNodeGroup.class);
                esNodeGroups.add(nodeGroup);
            }
            if (0 == response.getHits()
                    .getHits().length) {
                break;
            }
            try {
                response = elasticsearchConnection.getClient()
                        .scroll(new SearchScrollRequest(response.getScrollId()).scroll(new TimeValue(60000)),
                                RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new TableMapStoreException("Error while fetching node groups: ", e);
            }
        } while (response.getHits()
                .getHits().length != 0);
        log.debug("Fetched all node groups: {}", esNodeGroups);
        return esNodeGroups;
    }

    @Override
    public void delete(String groupName) {
        try {
            elasticsearchConnection.getClient()
                    .delete(new DeleteRequest(NODE_GROUP_INDEX, NODE_GROUP_TYPE, groupName), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("Error while deleting node group :{}", groupName, e);
            throw new NodeGroupStoreException("Error deleting node group: " + groupName, e);
        }
    }

}
