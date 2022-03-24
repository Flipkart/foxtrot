package com.flipkart.foxtrot.core.nodegroup;

import com.flipkart.foxtrot.common.nodegroup.AllocatedESNodeGroup;
import com.flipkart.foxtrot.common.nodegroup.SpecificTableAllocation;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.IndexTemplateMetaData;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils.getIndexPrefix;
import static org.awaitility.Awaitility.await;

public class AllocationManagerImplTest {

    private static final String TEST_INDEX = "foxtrot-test_consumer_app_android-table-10-8-2021";
    private static ElasticsearchConnection elasticsearchConnection;
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private AllocationManager allocationManager;

    @BeforeClass
    public static void setup() throws Exception {
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
        TestUtils.ensureIndex(elasticsearchConnection, TEST_INDEX);

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        elasticsearchConnection.stop();
    }

    @Before
    public void beforeMethod() throws Exception {
        allocationManager = new AllocationManagerImpl(elasticsearchConnection);
    }


    @Test
    public void shouldCreateAllocationTemplate() throws IOException {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("flipcast");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        allocationManager.createNodeAllocationTemplate(androidGroup);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(
                ElasticsearchUtils.getNodeAllocationTemplateName(androidGroup.getGroupName()));

        GetIndexTemplatesResponse indexTemplateResponse = elasticsearchConnection.getClient()
                .indices()
                .getIndexTemplate(getIndexTemplatesRequest, RequestOptions.DEFAULT);

        List<String> indexPatterns = tables1.stream()
                .map(table -> String.format("%s*", getIndexPrefix(table)))
                .collect(Collectors.toList());

        Assert.assertEquals(1, indexTemplateResponse.getIndexTemplates()
                .size());
        IndexTemplateMetaData indexTemplateMetaData = indexTemplateResponse.getIndexTemplates()
                .get(0);

        Assert.assertEquals(indexPatterns.size(), indexTemplateMetaData.patterns()
                .size());
        Assert.assertTrue(indexTemplateMetaData.patterns()
                .containsAll(indexPatterns));

        List<String> nodePatterns = indexTemplateMetaData.settings()
                .getAsList("index.routing.allocation.include._name");

        Assert.assertEquals(nodePatterns1.size(), nodePatterns.size());
        Assert.assertTrue(nodePatterns.containsAll(nodePatterns1));

        Assert.assertEquals(Integer.valueOf(4), indexTemplateMetaData.settings()
                .getAsInt("index.routing.allocation.total_shards_per_node", 0));

    }

    @Test
    public void shouldDeleteAllocationTemplate() throws IOException {
        exception.expect(ElasticsearchStatusException.class);
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("flipcast");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_delete")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        allocationManager.createNodeAllocationTemplate(androidGroup);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        allocationManager.deleteNodeAllocationTemplate(androidGroup.getGroupName());
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(
                ElasticsearchUtils.getNodeAllocationTemplateName(androidGroup.getGroupName()));

        GetIndexTemplatesResponse indexTemplateResponse = elasticsearchConnection.getClient()
                .indices()
                .getIndexTemplate(getIndexTemplatesRequest, RequestOptions.DEFAULT);


    }

    @Test
    public void shouldUpdateAllocationSettingsForIndex() throws IOException {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("flipcast");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("test_consumer_app_android")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        allocationManager.syncAllocationSettings(Sets.newHashSet(TEST_INDEX), androidGroup);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(TEST_INDEX);

        GetSettingsResponse settingsResponse = elasticsearchConnection.getClient()
                .indices()
                .getSettings(getSettingsRequest, RequestOptions.DEFAULT);
        Assert.assertEquals("elasticsearch1*,elasticsearch2*",
                settingsResponse.getSetting(TEST_INDEX, "index.routing.allocation.include._name"));

    }


}
