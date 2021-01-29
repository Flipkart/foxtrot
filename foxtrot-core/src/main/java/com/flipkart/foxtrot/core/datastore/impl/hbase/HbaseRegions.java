package com.flipkart.foxtrot.core.datastore.impl.hbase;

import com.flipkart.foxtrot.common.hbase.HRegionData;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.core.exception.HbaseRegionExtractionException;
import com.flipkart.foxtrot.core.exception.HbaseRegionMergeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;

import java.util.*;

@Slf4j
public class HbaseRegions {

    private static final long BYTES_IN_GB = 1073741824;
    private Connection connection;
    private Admin hBaseAdmin;

    public HbaseRegions(HbaseConfig hbaseConfig) {
        try {
            Configuration configuration = HBaseUtil.create(hbaseConfig);
            connection = ConnectionFactory.createConnection(configuration);
            this.hBaseAdmin = connection.getAdmin();
        } catch (Exception e) {
            log.info("Unable to get Hbase Admin!!!", e);
        }
    }

    public List<List<HRegionData>> getMergeableRegions(TableName tablename, double threshSizeInGB) {
        long threshSize = (long)(threshSizeInGB * BYTES_IN_GB);
        Map<String, HRegionData> hash = getRegionsMap(tablename);
        if (hash.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            HRegionData currentRegion = hash.get("");
            HRegionData nextRegion;

            List<List<HRegionData>> mergeRegions = new ArrayList<>();

            while (!StringUtils.isEmpty(currentRegion.getEndKey())) {
                nextRegion = hash.get(currentRegion.getEndKey());
                if (currentRegion.getRegionSize() + nextRegion.getRegionSize() <= threshSize) {
                    mergeRegions.add(Arrays.asList(currentRegion, nextRegion));
                    if (StringUtils.isEmpty(nextRegion.getEndKey())) {
                        break;
                    }
                    currentRegion = hash.get(nextRegion.getEndKey());
                } else {
                    currentRegion = nextRegion;
                }
            }
            return mergeRegions;
        } catch (Exception e) {
            throw new HbaseRegionMergeException("Unable to retrieve mergeable regions", e);
        }
    }

    public void mergeRegions(TableName tablename, double threshSizeInGB, int numberOfMerges) {
        long threshSize = (long)(threshSizeInGB * BYTES_IN_GB);
        if (numberOfMerges == -1) {
            numberOfMerges = Integer.MAX_VALUE;
        }
        Map<String, HRegionData> hash = getRegionsMap(tablename);
        if (hash.isEmpty()) {
            log.info("No regions to merge!!");
            return;
        }
        try {
            HRegionData currentRegion = hash.get("");
            HRegionData nextRegion;
            int count = 0;

            while (!StringUtils.isEmpty(currentRegion.getEndKey()) && count < numberOfMerges) {
                nextRegion = hash.get(currentRegion.getEndKey());
                if (currentRegion.getRegionSize() + nextRegion.getRegionSize() <= threshSize) {
                    log.info(String.format("Starting to Merge regions : %s startKey : %s endKey : %s and %s startKey : %s endKey : %s",
                            currentRegion.getRegionName(),
                            currentRegion.getStartKey(),
                            currentRegion.getEndKey(),
                            nextRegion.getRegionName(),
                            nextRegion.getStartKey(),
                            nextRegion.getEndKey()));
                    hBaseAdmin.mergeRegions(currentRegion.getEncodedNameAsBytes(), nextRegion.getEncodedNameAsBytes(), false);
                    log.info("Merged!!!");
                    count++;
                    if (StringUtils.isEmpty(nextRegion.getEndKey())) {
                        break;
                    }
                    currentRegion = hash.get(nextRegion.getEndKey());
                } else {
                    currentRegion = nextRegion;
                }
            }
        } catch (Exception e) {
            throw new HbaseRegionMergeException("Error merging regions!!", e);
        }
    }

    private Map<String, HRegionData> getRegionsMap(TableName tablename) {
        try {
            List<HRegionInfo> regions = getRegions(tablename);
            RegionLocator regionLocator = connection.getRegionLocator(tablename);
            RegionSizeCalculator regionSizeCalculator = new RegionSizeCalculator(regionLocator, hBaseAdmin);
            Map<String, HRegionData> regionsMap = new HashMap<>();

            if (CollectionUtils.isNullOrEmpty(regions)) {
                return Collections.emptyMap();
            }
            for (HRegionInfo region : regions) {
                regionsMap.put(new String(region.getStartKey()),
                        HRegionData.builder()
                                .regionName(region.getRegionNameAsString())
                                .startKey(new String(region.getStartKey()))
                                .endKey(new String(region.getEndKey()))
                                .encodedNameAsBytes(region.getEncodedNameAsBytes())
                                .regionSize(regionSizeCalculator.getRegionSize(region.getRegionName()))
                                .build()
                );
            }
            return regionsMap;
        } catch (Exception e) {
            throw new HbaseRegionExtractionException("Unable to retrieve region sizes", e);
        }
    }

    private List<HRegionInfo> getRegions(TableName tablename) {
        try {
            return hBaseAdmin.getTableRegions(tablename);
        } catch (Exception e) {
            throw new HbaseRegionExtractionException("Unable to retrieve regions", e);
        }
    }
}