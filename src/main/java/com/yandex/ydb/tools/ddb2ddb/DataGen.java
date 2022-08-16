package com.yandex.ydb.tools.ddb2ddb;

/**
 *
 * @author mzinal
 */
public class DataGen {
    
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DataGen.class);
    
    public void run() throws Exception {
        
    }
    
    public static void main(String[] args) {
        try {
            new DataGen().run();
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
            System.exit(1);
        }
    }
    
}
