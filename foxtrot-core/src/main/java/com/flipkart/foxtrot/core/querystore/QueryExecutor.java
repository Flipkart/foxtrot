package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.query.CachableResponseGenerator;
import com.flipkart.foxtrot.core.common.Action;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 12:51 PM
 */
public class QueryExecutor {
    private ExecutorService executorService;

    public QueryExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public <P extends CachableResponseGenerator,T extends Serializable> T execute(
                                            Action<P,T> action) throws QueryStoreException {
        return action.execute();
    }

    public String executeAsync(Action action) {
        return action.execute(executorService);
    }
}
