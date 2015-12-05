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
package com.flipkart.foxtrot.core.datastore;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:18 PM
 */
public class DataStoreException extends Exception {
    public enum ErrorCode {
        TABLE_NOT_FOUND,
        STORE_CONNECTION,
        STORE_SINGLE_SAVE,
        STORE_MULTI_SAVE,
        STORE_SINGLE_GET,
        STORE_MULTI_GET,
        STORE_INVALID_DOCUMENT,
        STORE_INVALID_REQUEST,
        STORE_NO_DATA_FOUND_FOR_ID,
        STORE_NO_DATA_FOUND_FOR_IDS
    }

    private final ErrorCode errorCode;

    public DataStoreException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public DataStoreException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("errorCode", errorCode)
                .toString();
    }
}
