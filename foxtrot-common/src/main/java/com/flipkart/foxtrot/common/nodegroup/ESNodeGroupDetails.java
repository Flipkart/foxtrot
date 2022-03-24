package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.TreeMap;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ESNodeGroupDetails {

    private int nodeCount;

    private TreeMap<String, DiskUsageInfo> nodeInfo;

    @JsonUnwrapped
    private DiskUsageInfo diskUsageInfo;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DiskUsageInfo {

        private String totalDiskStorage;

        private String usedDiskStorage;

        private String availableDiskStorage;

        private String usedDiskPercentage;
    }
}
