package com.flipkart.foxtrot.sql.query;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.sql.FqlQuery;
import com.flipkart.foxtrot.sql.FqlQueryType;
import com.flipkart.foxtrot.sql.FqlQueryVisitor;

import java.util.List;

public class FqlActionQuery extends FqlQuery {
    private final ActionRequest actionRequest;
    private final List<String> selectedFields;

    public FqlActionQuery(final FqlQueryType type, final ActionRequest actionRequest, List<String> selectedFields) {
        super(type);
        this.actionRequest = actionRequest;
        this.selectedFields = selectedFields;
    }

    @Override
    public void receive(FqlQueryVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public ActionRequest getActionRequest() {
        return actionRequest;
    }

    public List<String> getSelectedFields() {
        return selectedFields;
    }
}
