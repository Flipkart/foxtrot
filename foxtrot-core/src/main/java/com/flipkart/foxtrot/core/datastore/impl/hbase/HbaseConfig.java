package com.flipkart.foxtrot.core.datastore.impl.hbase;

import org.hibernate.validator.constraints.NotEmpty;

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
}
