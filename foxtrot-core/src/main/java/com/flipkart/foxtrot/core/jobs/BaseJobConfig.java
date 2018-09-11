package com.flipkart.foxtrot.core.jobs;
/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/***
 Created by nitish.goyal on 11/09/18
 ***/
@NoArgsConstructor
@Data
@AllArgsConstructor
public class BaseJobConfig {

    @Min(3600)
    private int interval;

    /*
    Initial day in hours. Used to run the config at ith hour of the day
     */
    @Min(1)
    private int initialDelay;

    @NotNull
    private boolean active;

    private String jobName;

    private int maxTimeToRunJobInMinutes;
}
