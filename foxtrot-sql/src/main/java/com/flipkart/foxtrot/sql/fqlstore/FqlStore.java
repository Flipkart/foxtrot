package com.flipkart.foxtrot.sql.fqlstore;

import lombok.Data;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class FqlStore {
    public static final String TITLE = "title";
    private String id;

    private String title;

    private String query;
}
