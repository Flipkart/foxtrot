/**
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
package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

public class ConsolePersistenceException extends FoxtrotException {

    private String consoleId;

    public ConsolePersistenceException(String consoleId, String message) {
        super(ErrorCode.CONSOLE_SAVE_EXCEPTION, message);
        this.consoleId = consoleId;
    }

    public ConsolePersistenceException(String consoleId, String message, Throwable cause) {
        super(ErrorCode.CONSOLE_SAVE_EXCEPTION, message, cause);
        this.consoleId = consoleId;
    }

    public String getConsoleId() {
        return consoleId;
    }

    public void setConsoleId(String consoleId) {
        this.consoleId = consoleId;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("consoleId", this.consoleId);
        map.put("message", this.getCause().getMessage());
        return map;
    }
}
