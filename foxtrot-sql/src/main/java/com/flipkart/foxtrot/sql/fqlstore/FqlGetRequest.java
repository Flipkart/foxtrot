package com.flipkart.foxtrot.sql.fqlstore;

import lombok.Data;


/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class FqlGetRequest {

    private String title;
    private String userId;
    private int from = 0;
    private int size = 20;
}
