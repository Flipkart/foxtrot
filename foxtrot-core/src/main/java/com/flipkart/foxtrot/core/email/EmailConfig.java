package com.flipkart.foxtrot.core.email;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.Email;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    @Email
    private String from;
    private String url;
    private List<String> eventNotificationEmails;

    private boolean slowQueryEmailEnabled = true;
    private Set<String> slowQueryEmailBlacklistOpCodes = new HashSet<>();
    private boolean blockedQueryEmailEnabled = true;
    private boolean cardinalityCalculationFailureEmailEnabled = true;
}
