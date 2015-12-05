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
package com.flipkart.foxtrot.core.querystore;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:18 PM
 */
public class QueryStoreException extends Exception {
    public QueryStoreException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public QueryStoreException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public enum ErrorCode {
        NO_SUCH_TABLE,
        DOCUMENT_SAVE_ERROR,
        DOCUMENT_GET_ERROR,
        QUERY_EXECUTION_ERROR,
        QUERY_MALFORMED_QUERY_ERROR,
        HISTOGRAM_GENERATION_ERROR,
        UNRESOLVABLE_OPERATION,
        ACTION_RESOLUTION_ERROR,
        METADATA_FETCH_ERROR,
        DOCUMENT_NOT_FOUND,
        INVALID_REQUEST,
        NO_METADATA_FOUND,
        TABLE_LIST_FETCH_ERROR,
        DATA_CLEANUP_ERROR,
        EXECUTION_ERROR

    }

    private final ErrorCode errorCode;

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
