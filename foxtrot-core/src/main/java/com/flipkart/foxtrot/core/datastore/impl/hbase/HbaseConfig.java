/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.datastore.impl.hbase;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 13/03/14
 * Time: 7:35 PM
 */
public class HbaseConfig {

    private boolean secure = false;
    private String keytabFileName;
    private String coreSite;
    private String hdfsSite;
    private String hbasePolicy;
    private String hbaseSite;
    private String kerberosConfigFile;
    private String kinitPath;
    private String authString;
    private String seggregatedTablePrefix;
    private String hbaseZookeeperQuorum;
    private Integer hbaseZookeeperClientPort;

    @Min(1)
    @Max(Byte.MAX_VALUE)
    private short numBuckets = 32;

    private String rawKeyVersion = "2.0";

    @NotNull
    @NotEmpty
    private String tableName;

    public HbaseConfig() {
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public String getKeytabFileName() {
        return keytabFileName;
    }

    public void setKeytabFileName(String keytabFileName) {
        this.keytabFileName = keytabFileName;
    }

    public String getCoreSite() {
        return coreSite;
    }

    public void setCoreSite(String coreSite) {
        this.coreSite = coreSite;
    }

    public String getHdfsSite() {
        return hdfsSite;
    }

    public void setHdfsSite(String hdfsSite) {
        this.hdfsSite = hdfsSite;
    }

    public String getHbasePolicy() {
        return hbasePolicy;
    }

    public void setHbasePolicy(String hbasePolicy) {
        this.hbasePolicy = hbasePolicy;
    }

    public String getHbaseSite() {
        return hbaseSite;
    }

    public void setHbaseSite(String hbaseSite) {
        this.hbaseSite = hbaseSite;
    }

    public String getKerberosConfigFile() {
        return kerberosConfigFile;
    }

    public void setKerberosConfigFile(String kerberosConfigFile) {
        this.kerberosConfigFile = kerberosConfigFile;
    }

    public String getKinitPath() {
        return kinitPath;
    }

    public void setKinitPath(String kinitPath) {
        this.kinitPath = kinitPath;
    }

    public String getAuthString() {
        return authString;
    }

    public void setAuthString(String authString) {
        this.authString = authString;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSeggregatedTablePrefix() {
        return seggregatedTablePrefix;
    }

    public void setSeggregatedTablePrefix(String seggregatedTablePrefix) {
        this.seggregatedTablePrefix = seggregatedTablePrefix;
    }

    public Integer getHbaseZookeeperClientPort() {
        return hbaseZookeeperClientPort;
    }

    public void setHbaseZookeeperClientPort(Integer hbaseZookeeperClientPort) {
        this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
    }

    public String getHbaseZookeeperQuorum() {
        return hbaseZookeeperQuorum;
    }

    public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
        this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    }

    public String getRawKeyVersion() {
        return rawKeyVersion;
    }

    public void setRawKeyVersion(String rawKeyVersion) {
        this.rawKeyVersion = rawKeyVersion;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(short numBuckets) {
        this.numBuckets = numBuckets;
    }
}
