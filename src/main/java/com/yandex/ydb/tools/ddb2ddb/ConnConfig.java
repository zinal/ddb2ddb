package com.yandex.ydb.tools.ddb2ddb;

import java.util.Properties;

/**
 * DynamoDB connection configuration.
 * @author mzinal
 */
public class ConnConfig {
    
    private String endpoint;
    private String region;
    private String keyId;
    private String keySecret;
    
    public ConnConfig() {
    }
    
    public ConnConfig(Properties props, String prefix) {
        this.endpoint   = props.getProperty(prefix + ".endpoint");
        this.region     = props.getProperty(prefix + ".region");
        this.keyId      = props.getProperty(prefix + ".keyId");
        this.keySecret  = props.getProperty(prefix + ".keySecret");
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
    
    public boolean hasKey() {
        return (keyId!=null) && (keySecret!=null);
    }

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public String getKeySecret() {
        return keySecret;
    }

    public void setKeySecret(String keySecret) {
        this.keySecret = keySecret;
    }
    
}
