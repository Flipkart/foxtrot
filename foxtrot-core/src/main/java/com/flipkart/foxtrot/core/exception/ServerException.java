package com.flipkart.foxtrot.core.exception;

import com.google.common.collect.Maps;
<<<<<<< HEAD
import java.util.Map;
import lombok.Getter;
=======

import java.util.Map;
>>>>>>> phonepe-develop

/**
 * Created by rishabh.goyal on 19/12/15.
 */
<<<<<<< HEAD
@Getter
public class ServerException extends FoxtrotException {

    private final String message;
=======
public class ServerException extends FoxtrotException {

    private String message;
>>>>>>> phonepe-develop

    protected ServerException(String message, Throwable cause) {
        super(ErrorCode.EXECUTION_EXCEPTION, cause);
        this.message = message;
    }

    @Override
<<<<<<< HEAD
=======
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
>>>>>>> phonepe-develop
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("message", this.message);
        return map;
    }
}
