package com.flipkart.foxtrot.pipeline.processors;

public class ProcessorExecutionException extends RuntimeException {

    public ProcessorExecutionException(String message,
                                       Throwable cause) {
        super(message, cause);
    }
}
