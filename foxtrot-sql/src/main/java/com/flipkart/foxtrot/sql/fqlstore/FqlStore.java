package com.flipkart.foxtrot.sql.fqlstore;

import lombok.Data;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class FqlStore {

    public static final String TITLE_FIELD = "title";
    public static final String USER_ID = "userId";
    private String id;
    private String userId;

    private String title;

    private String query;
}
