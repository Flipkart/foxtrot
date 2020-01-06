package com.flipkart.foxtrot.server.guice;

import com.flipkart.foxtrot.core.querystore.mutator.IndexerEventMutator;
import com.flipkart.foxtrot.core.querystore.mutator.LargeTextNodeRemover;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import java.util.List;

public class IndexerEventMutatorListProvider implements Provider<List<IndexerEventMutator>> {

    @Inject
    private Provider<LargeTextNodeRemover> largeTextNodeRemoverProvider;

    public List<IndexerEventMutator> get() {
        return Lists.newArrayList(largeTextNodeRemoverProvider.get());
    }
}
