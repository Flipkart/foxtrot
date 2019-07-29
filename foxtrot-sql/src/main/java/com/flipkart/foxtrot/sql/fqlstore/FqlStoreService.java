package com.flipkart.foxtrot.sql.fqlstore;


import java.util.List;

/***
 Created by mudit.g on Jan, 2019
 ***/
public interface FqlStoreService {

    void save(FqlStore fqlStore);

    List<FqlStore> get(FqlGetRequest fqlGetRequest);
}
