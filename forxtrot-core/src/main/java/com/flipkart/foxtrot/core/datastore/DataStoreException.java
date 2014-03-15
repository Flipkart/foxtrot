package com.flipkart.foxtrot.core.datastore;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:18 PM
 */
public class DataStoreException extends Exception {
    public static enum ErrorCode {
        STORE_CONNECTION,
        STORE_SINGLE_SAVE,
        STORE_MULTI_SAVE,
        STORE_SINGLE_GET,
        STORE_MULTI_GET,
        STORE_NO_DATA_FOUND_FOR_ID,
        STORE_NO_DATA_FOUND_FOR_IDS,
        STORE_CLOSE,
    }

    private ErrorCode errorCode;

    public DataStoreException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public DataStoreException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }


}
