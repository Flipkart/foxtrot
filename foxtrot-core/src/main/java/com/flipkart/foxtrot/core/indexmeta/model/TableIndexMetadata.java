package com.flipkart.foxtrot.core.indexmeta.model;

import com.flipkart.foxtrot.common.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TableIndexMetadata implements Serializable {

    private static final long serialVersionUID = 6374568724810679330L;

    // document id for ES document and key for hazelcast map
    private String indexName;

    private String table;

    private String datePostFix;

    private long shardCount;

    private double totalIndexSizeInTBs;

    private double totalIndexSizeInGBs;

    private double totalIndexSizeInMBs;

    private long totalIndexSizeInBytes;

    private double averageShardSizeInGBs;

    private double averageShardSizeInMBs;

    private long averageShardSizeInBytes;

    private long noOfColumns;

    private long noOfEvents;

    private double averageEventSizeInBytes;

    private double averageEventSizeInKBs;

    private double averageEventSizeInMBs;

    // date postfix converted to epoch millis e.g. 11-2-2021 00:00
    private long timestamp;

    private long updatedAt;

    private Date date;
}
