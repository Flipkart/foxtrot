package com.flipkart.foxtrot.core.alerts;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 06/10/18
 ***/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EmailConfig {

    private String host = "127.0.0.1";
    private String user;
    private String password;
    private int port;
    private String from;
    private String url;
}
