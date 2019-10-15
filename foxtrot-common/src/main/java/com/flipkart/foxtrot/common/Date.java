package com.flipkart.foxtrot.common;
/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.io.Serializable;

/***
 Created by nitish.goyal on 29/11/18
 ***/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Date implements Serializable {

    private int year;
    private int monthOfYear;
    private int dayOfWeek;
    private int dayOfMonth;
    private int hourOfDay;
    private int minuteOfHour;
    private int minuteOfDay;
    private int weekOfYear;

    public Date(DateTime dateTime) {

        this.year = dateTime.getYear();
        this.monthOfYear = dateTime.getMonthOfYear();
        this.dayOfWeek = dateTime.getDayOfWeek();
        this.dayOfMonth = dateTime.getDayOfMonth();
        this.hourOfDay = dateTime.getHourOfDay();
        this.minuteOfHour = dateTime.getMinuteOfHour();
        this.minuteOfDay = dateTime.getMinuteOfDay();
        this.weekOfYear = dateTime.getWeekOfWeekyear();
    }
}
