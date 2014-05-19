package com.flipkart.foxtrot.server.console;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Vector;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

public class ElasticsearchConsolePersistence implements ConsolePersistence {
    private static final String INDEX = "consoles";
    private static final String TYPE = "console_data";

    private ElasticsearchConnection connection;
    private ObjectMapper mapper;

    public ElasticsearchConsolePersistence(ElasticsearchConnection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public void save(Console console) throws ConsolePersistenceException {
        try {
            connection.getClient()
                    .prepareIndex()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(console.getId())
                    .setSource(mapper.writeValueAsBytes(console))
                    .setRefresh(true)
                    .setConsistencyLevel(WriteConsistencyLevel.ALL)
                    .execute()
                    .get();
        } catch (Exception e) {
            throw new ConsolePersistenceException("Error saving console: " + console.getId(), e);
        }
    }

    @Override
    public Console get(String id) throws ConsolePersistenceException {
        try {
            GetResponse result = connection.getClient()
                    .prepareGet()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(id)
                    .execute()
                    .actionGet();
            if(!result.isExists()) {
                return null;
            }
            return mapper.readValue(result.getSourceAsBytes(), Console.class);
        } catch (Exception e) {
            throw new ConsolePersistenceException("Error saving console: " + id, e);
        }
    }

    @Override
    public List<Console> get() throws ConsolePersistenceException {
        SearchResponse response = connection.getClient().prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(boolQuery().must(matchAllQuery()))
                .addSort(fieldSort("name").order(SortOrder.DESC))
                .setScroll(new TimeValue(60000))
                .setSearchType(SearchType.SCAN)
                .execute()
                .actionGet();
        try {
            Vector<Console> results = new Vector<Console>();
            while (true) {
                response = connection.getClient().prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                SearchHits hits = response.getHits();
                for(SearchHit hit:hits) {
                    results.add(mapper.readValue(hit.sourceAsString(), Console.class));
                }
                if(0 == response.getHits().hits().length) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            throw new ConsolePersistenceException("Error getting consoles: ", e);
        }
    }
}
