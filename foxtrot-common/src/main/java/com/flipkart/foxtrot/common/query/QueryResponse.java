package com.flipkart.foxtrot.common.query;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Document;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 24/03/14
 * Time: 1:00 PM
 */
public class QueryResponse implements ActionResponse {
    private List<Document> documents;

    public QueryResponse() {
    }

    public QueryResponse(List<Document> documents) {
        this.documents = documents;
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public void setDocuments(List<Document> documents) {
        this.documents = documents;
    }
}
