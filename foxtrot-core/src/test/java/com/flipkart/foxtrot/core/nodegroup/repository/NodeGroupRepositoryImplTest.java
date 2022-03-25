package com.flipkart.foxtrot.core.nodegroup.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.flipkart.foxtrot.common.nodegroup.*;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class NodeGroupRepositoryImplTest {

    private static ElasticsearchConnection elasticsearchConnection;
    private NodeGroupRepository nodeGroupRepository;

    @BeforeClass
    public static void setup() throws Exception {
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        elasticsearchConnection.stop();
    }

    @Before
    public void beforeMethod() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS);
        SerDe.init(objectMapper);
        nodeGroupRepository = new NodeGroupRepositoryImpl(elasticsearchConnection);
    }

    @After
    public void afterMethod() throws IOException {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(NodeGroupRepositoryImpl.NODE_GROUP_INDEX);
        elasticsearchConnection.getClient()
                .indices()
                .delete(deleteRequest);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
    }

    @Test
    public void shouldSaveNodeGroup() {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("flipcast");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("group_save")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get(androidGroup.getGroupName()));

        List<ESNodeGroup> esNodeGroups = nodeGroupRepository.getAll();
        Assert.assertEquals(1, esNodeGroups.size());
        Assert.assertTrue(esNodeGroups.contains(androidGroup));
    }

    @Test
    public void shouldGetVacantNodeGroup() {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch100.test.nmx");
        nodePatterns1.add("elasticsearch101.test.nmx");

        VacantESNodeGroup vacantESNodeGroup = VacantESNodeGroup.builder()
                .groupName("vacant_test")
                .nodePatterns(nodePatterns1)
                .build();

        nodeGroupRepository.save(vacantESNodeGroup);

        Assert.assertEquals(vacantESNodeGroup, nodeGroupRepository.getVacantGroup());

    }

    @Test
    public void shouldDeleteNodeGroup() {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch1*");
        nodePatterns1.add("elasticsearch2*");

        SortedSet<String> tables1 = new TreeSet<>();
        tables1.add("test_consumer_app_android");
        tables1.add("flipcast");

        AllocatedESNodeGroup androidGroup = AllocatedESNodeGroup.builder()
                .groupName("group_to_delete")
                .nodePatterns(nodePatterns1)
                .tableAllocation(SpecificTableAllocation.builder()
                        .totalShardsPerNode(4)
                        .tables(tables1)
                        .build())
                .build();

        nodeGroupRepository.save(androidGroup);

        Assert.assertEquals(androidGroup, nodeGroupRepository.get(androidGroup.getGroupName()));
        nodeGroupRepository.delete(androidGroup.getGroupName());
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        List<ESNodeGroup> esNodeGroups = nodeGroupRepository.getAll();
        Assert.assertEquals(0, esNodeGroups.size());
    }

    @Test
    public void shouldGetCommonNodeGroup() {
        SortedSet<String> nodePatterns1 = new TreeSet<>();
        nodePatterns1.add("elasticsearch100.test.nmx");
        nodePatterns1.add("elasticsearch101.test.nmx");

        AllocatedESNodeGroup commonGroup = AllocatedESNodeGroup.builder()
                .groupName("common")
                .nodePatterns(nodePatterns1)
                .tableAllocation(new CommonTableAllocation())
                .build();

        nodeGroupRepository.save(commonGroup);

        Assert.assertEquals(commonGroup, nodeGroupRepository.getCommonGroup());

    }
}
