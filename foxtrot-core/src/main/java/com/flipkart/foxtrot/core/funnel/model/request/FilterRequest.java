package com.flipkart.foxtrot.core.funnel.model.request;

import com.flipkart.foxtrot.common.query.Filter;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.sort.SortOrder;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Getter
public class FilterRequest {

    private List<Filter> filters;

    private int page;
    private int size;

    private String fieldName;
    private SortOrder sortOrder;

    public FilterRequest() {
        this.page = 1;
        this.size = 10;
        this.fieldName = "id";
        this.sortOrder = SortOrder.DESC;
    }

    @Builder
    public FilterRequest(List<Filter> filters, int page, int size, String fieldName, SortOrder sortOrder) {
        this.filters = filters;
        if (page == 0) {
            this.page = 1;
        } else {
            this.page = page;
        }
        if (size == 0) {
            this.size = 10;
        } else {
            this.size = size;
        }
        if (Strings.isNullOrEmpty(fieldName)) {
            this.fieldName = "id";
        } else {
            this.fieldName = fieldName;
        }
        if (null == sortOrder) {
            this.sortOrder = SortOrder.DESC;
        } else {
            this.sortOrder = sortOrder;
        }
    }

    public int getFrom() {
        return (page - 1) * size;
    }
}
