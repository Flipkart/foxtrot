package com.flipkart.foxtrot.pipeline;

public abstract class PipelineConstants {

    public static final String REFLECTION_BASED_PROCESSOR_FACTORY = "REFLECTION_BASED_PROCESSOR_FACTORY";
    public static final String PROCESSOR_CACHE_BUILDER = "PROCESSOR_CACHE_BUILDER";

    private PipelineConstants() {
    }

    public abstract static class HandlerCache {

        public static final String CACHE_SPEC = "maximumSize=1000,expireAfterAccess=30m";

        private HandlerCache() {
        }
    }
}
