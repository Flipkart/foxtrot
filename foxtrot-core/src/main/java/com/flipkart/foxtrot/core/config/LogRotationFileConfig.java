package com.flipkart.foxtrot.core.config;

import javax.validation.constraints.NotNull;

/**
 * Created by anzer.moid on 10/02/18.
 */
public class LogRotationFileConfig {


    @NotNull
    private int minIndex;

    @NotNull
    private int maxIndex;

    @NotNull
    private String maxFileSize;

    @NotNull
    private String filePattern;

    public int getMinIndex() {
        return minIndex;
    }

    public int getMaxIndex() {
        return maxIndex;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public String getFilePattern() {
        return filePattern;
    }

    @Override
    public String toString() {
        return "LogRotationFileConfig{" + "minIndex=" + minIndex + ", maxIndex=" + maxIndex + ", maxFileSize='"
                + maxFileSize + '\'' + ", filePattern='" + filePattern + '\'' + '}';
    }

}