package com.flipkart.foxtrot.sql.fqlstore;

import com.flipkart.foxtrot.common.query.Filter;

import java.util.List;

/***
 Created by mudit.g on Jan, 2019
 ***/
public interface FqlStoreService {
    void save(FqlStore fqlStore);

    List<FqlStore> get(FilterRequest filterRequest) throws Exception;
}
