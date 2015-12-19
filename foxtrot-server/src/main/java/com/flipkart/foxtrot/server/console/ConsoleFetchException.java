package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public class ConsoleFetchException extends FoxtrotException {

    public ConsoleFetchException(String message, Throwable cause) {
        super(ErrorCode.CONSOLE_FETCH_EXCEPTION, message, cause);
    }
}
