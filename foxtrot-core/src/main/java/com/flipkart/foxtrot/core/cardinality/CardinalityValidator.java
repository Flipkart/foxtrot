package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.common.Action;

import java.util.List;

public interface CardinalityValidator {

    void validateCardinality(Action action,
                             ActionRequest actionRequest,
                             String table,
                             List<String> groupingColumns);

}
