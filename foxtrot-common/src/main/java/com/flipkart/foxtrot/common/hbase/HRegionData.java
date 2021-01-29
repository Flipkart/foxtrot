package com.flipkart.foxtrot.common.hbase;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HRegionData {
    String regionName;
    String startKey;
    String endKey;
    byte[] encodedNameAsBytes;
    long regionSize;
}
