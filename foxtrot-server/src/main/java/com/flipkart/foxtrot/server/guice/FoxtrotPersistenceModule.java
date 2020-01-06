package com.flipkart.foxtrot.server.guice;

import com.flipkart.foxtrot.core.cache.CacheFactory;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import java.util.List;
import java.util.Set;

public class FoxtrotPersistenceModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DataStore.class).to(HBaseDataStore.class);
        bind(QueryStore.class).to(ElasticsearchQueryStore.class);
        bind(TableMetadataManager.class).to(DistributedTableMetadataManager.class);
        bind(TableManager.class).to(FoxtrotTableManager.class);
        bind(CacheFactory.class).to(DistributedCacheFactory.class);
        bind(ConsolePersistence.class).to(ElasticsearchConsolePersistence.class);
        bind(new TypeLiteral<List<IndexerEventMutator>>() {
        }).toProvider(IndexerEventMutatorListProvider.class);
    }
}
