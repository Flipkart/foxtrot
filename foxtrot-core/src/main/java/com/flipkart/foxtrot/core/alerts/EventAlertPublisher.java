package com.flipkart.foxtrot.core.alerts;
/*
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.phonepe.models.fabric.*;

/***
 Created by nitish.goyal on 06/10/18
 ***/
public class EventAlertPublisher implements AlertPublisher<Boolean> {

    private final EmailClient emailClient;

    public EventAlertPublisher(EmailClient emailClient) {
        this.emailClient = emailClient;
    }

    @Override
    public Boolean send(SlackAlertEvent slackAlertEvent) {
        return null;
    }

    @Override
    public Boolean send(SmsAlertEvent smsAlertEvent) {
        return null;
    }

    @Override
    public Boolean send(EmailAlertEvent emailAlertEvent) {
        return emailClient.sendEmail(emailAlertEvent);
    }

    @Override
    public Boolean send(HangoutsAlertEvent hangoutsAlertEvent) {
        return null;
    }
}
