package com.flipkart.foxtrot.core.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.pipeline.PipelineMetadataManagerPipelineFetcherAdaptor;
import com.flipkart.foxtrot.core.pipeline.impl.DistributedPipelineMetadataManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.impl.ElasticsearchTestUtils;
import com.flipkart.foxtrot.pipeline.resolver.PipelineFetcher;
import com.flipkart.foxtrot.pipeline.resolver.PipelineFetcherTest;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

@Ignore
public class PipelineMetadataManagerPipelineFetcherAdaptorTest extends PipelineFetcherTest {

    private static HazelcastInstance hazelcastInstance;
    private static ElasticsearchConnection elasticsearchConnection;

    @BeforeClass
    public static void setupClass() throws Exception {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());
        elasticsearchConnection = ElasticsearchTestUtils.getConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
        elasticsearchConnection.stop();
    }

    @Override
    protected PipelineFetcher underTest() throws Exception {
        val objectMapper = new ObjectMapper();
        objectMapper.registerSubtypes(GroupResponse.class);
        SerDe.init(objectMapper);

        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        hazelcastConnection.start();

        val distributedPipelineMetadataManager = new DistributedPipelineMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        distributedPipelineMetadataManager.start();
        distributedPipelineMetadataManager.save(PipelineFetcherTest.pipeline);
        return new PipelineMetadataManagerPipelineFetcherAdaptor(distributedPipelineMetadataManager);
    }
}
