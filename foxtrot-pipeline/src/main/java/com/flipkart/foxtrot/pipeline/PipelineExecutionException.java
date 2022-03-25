package com.flipkart.foxtrot.pipeline;

public class PipelineExecutionException extends Exception {

    public PipelineExecutionException(String message,
                                      Exception e) {
        super(message, e);
    }
}
