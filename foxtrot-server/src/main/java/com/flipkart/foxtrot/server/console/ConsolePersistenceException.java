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
package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;

@Getter
public class ConsolePersistenceException extends FoxtrotException {

    private final String consoleId;

    public ConsolePersistenceException(String consoleId, String message) {
        super(ErrorCode.CONSOLE_SAVE_EXCEPTION, message);
        this.consoleId = consoleId;
    }

    public ConsolePersistenceException(String consoleId, String message, Throwable cause) {
        super(ErrorCode.CONSOLE_SAVE_EXCEPTION, message, cause);
        this.consoleId = consoleId;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("consoleId", this.consoleId);
        map.put("message", this.getCause()
                .getMessage());
        return map;
    }
}
