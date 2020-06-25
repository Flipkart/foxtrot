package com.flipkart.foxtrot.core.cardinality;

import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.core.querystore.actions.GroupAction;

public interface CardinalityValidator {

    void validateCardinality(GroupAction groupAction, GroupRequest groupRequest);

}
