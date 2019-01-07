package com.flipkart.foxtrot.sql.fqlstore;

import com.flipkart.foxtrot.common.query.Filter;
import lombok.Data;

import java.util.List;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class FilterRequest {
    private List<Filter> filters;

    private int from = 0;
    private int size = 10;
}
