package com.flipkart.foxtrot.server.config;

import lombok.Data;

/***
 Created by nitish.goyal on 15/05/19
 ***/
@Data
public class GandalfConfiguration {

    private String redirectUrl;

    private String serviceBaseUrl;

    private String username;

    private String password;
}
