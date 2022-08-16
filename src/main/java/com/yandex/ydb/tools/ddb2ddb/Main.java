package com.yandex.ydb.tools.ddb2ddb;

import java.io.FileInputStream;
import java.util.Properties;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 *
 * @author mzinal
 */
public class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);

    private final ConnConfig sourceConfig;
    private final ConnConfig targetConfig;
    private final String sourceTable;
    private final String targetTable;

    public Main(Properties jobFile) {
        this.sourceConfig = new ConnConfig(jobFile, "source");
        this.targetConfig = new ConnConfig(jobFile, "target");
        this.sourceTable = jobFile.getProperty("source.tableName");
        this.targetTable = jobFile.getProperty("target.tableName", this.sourceTable);
    }

    public void run() throws Exception {
        copyTableDefinition();
    }

    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                throw new Exception("Missing or extra input argument: jobfile.properties");
            }
            final Properties jobFile = new Properties();
            try (FileInputStream fis = new FileInputStream(args[0])) {
                jobFile.loadFromXML(fis);
            }
            LOG.info("DynamoDB data copy tool starting...");
            new Main(jobFile).run();
            LOG.info("DynamoDB data copy tool completed!");
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
            System.exit(1);
        }
    }

    private void copyTableDefinition() {
        try (DynamoDbClient sourceClient = sourceConfig.newClient()) {
            LOG.info("Looking for the source table {} ...", sourceTable);
            TableDescription td1 = Helpers.describeTable(sourceClient, sourceTable);
            if (td1 == null) {
                throw new IllegalArgumentException("Source table does not exist: " + sourceTable);
            }
            try (DynamoDbClient targetClient = targetConfig.newClient()) {
                LOG.info("Looking for the target table {} ...", targetTable);
                TableDescription td2 = Helpers.describeTable(targetClient, targetTable);
                if (td2 == null) {
                    LOG.info("Creating target table {} ...", targetTable);
                    td2 = createTable(targetClient, td1, targetTable);
                }
                // TODO: validate that table keys are the same
                // Wait for the target table to become ready
                boolean loggedStatus = false;
                while (TableStatus.ACTIVE != td2.tableStatus()) {
                    if (!loggedStatus) {
                        LOG.info("Waiting for target table {} ...", targetTable);
                        loggedStatus = true;
                    }
                    try { Thread.sleep(500L); } catch(InterruptedException ix) {}
                    td2 = Helpers.describeTable(targetClient, targetTable);
                    if (td2 == null)
                        throw new IllegalStateException("Target table disappeared: " + targetTable);
                }
            } // source
        } // target
        LOG.info("Source and target tables validated!");
    }

    private TableDescription createTable(DynamoDbClient client, TableDescription td, String tableName) {
        CreateTableRequest ctr = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(td.attributeDefinitions())
                .keySchema(td.keySchema())
                .provisionedThroughput(convert(td.provisionedThroughput()))
                .build();
        return client.createTable(ctr).tableDescription();
    }
    
    private ProvisionedThroughput convert(ProvisionedThroughputDescription x) {
        ProvisionedThroughput.Builder b = ProvisionedThroughput.builder();
        if (x==null) {
            b.readCapacityUnits(10L);
            b.writeCapacityUnits(10L);
        } else {
            b.readCapacityUnits(safe(x.readCapacityUnits(), 10L));
            b.writeCapacityUnits(safe(x.writeCapacityUnits(), 10L));
        }
        return b.build();
    }
    
    private long safe(Long v, long def) {
        if (v==null || v<=0L)
            return def;
        return v;
    }

}
