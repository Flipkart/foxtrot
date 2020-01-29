package com.flipkart.foxtrot.server.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 10/09/19
 ***/
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class QueryConfig {

    @Default
    private boolean logQueries = false;

    @Default
    private boolean blockConsoleQueries = false;

}
