package com.flipkart.foxtrot.common;
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

import lombok.Data;
import org.joda.time.DateTime;

/***
 Created by nitish.goyal on 29/11/18
 ***/
@Data
public class Time {

    private int year;
    private int month;
    private int dayOfWeek;
    private int date;
    private int hour;
    private int minuteOfHour;
    private int minuteOfDay;

    public Time() {
        DateTime now = DateTime.now();
        this.year = now.getYear();
        this.month = now.getMonthOfYear();
        this.dayOfWeek = now.getDayOfWeek();
        this.date = now.getDayOfMonth();
        this.hour = now.getHourOfDay();
        this.minuteOfHour = now.getMinuteOfHour();
        this.minuteOfDay = now.getMinuteOfDay();
    }
}
