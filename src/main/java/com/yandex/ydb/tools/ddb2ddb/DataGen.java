package com.yandex.ydb.tools.ddb2ddb;

import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 *
 * @author mzinal
 */
public class DataGen {
    
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DataGen.class);
    
    private final ConnConfig config;
    private final String tableName;
    
    public DataGen(Properties jobFile) {
        this.config = new ConnConfig(jobFile, "datagen");
        this.tableName = jobFile.getProperty("datagen.tableName", "MyTable");
    }

    public void run() throws Exception {
        try (DynamoDbClient client = initClient()) {
            run(client);
        }
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
            LOG.info("DataGen starting...");
            new DataGen(jobFile).run();
            LOG.info("DataGen completed!");
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
            System.exit(1);
        }
    }

    private DynamoDbClient initClient() {
        DynamoDbClientBuilder builder = DynamoDbClient.builder();
        builder.endpointOverride(URI.create(config.getEndpoint()));
        builder.region(Region.of(config.getRegion()));
        if (config.hasKey()) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(config.getKeyId(), config.getKeySecret())));
        }
        return builder.build();
    }

    private void run(DynamoDbClient client) throws Exception {
        TableDescription td;
        if (null == (td = describeTable(client, tableName))) {
            td = createTable(client, tableName);
        }
        boolean loggedStatus = false;
        while (TableStatus.ACTIVE != td.tableStatus()) {
            if (!loggedStatus) {
                LOG.info("Waiting for table {} ...", tableName);
                loggedStatus = true;
            }
            try { Thread.sleep(500L); } catch(InterruptedException ix) {}
            td = describeTable(client, tableName);
            if (td == null)
                throw new IllegalStateException("Table disappeared: " + tableName);
        }
        LOG.info("Writing data to table {} ...", tableName);
        for (int i=0; i<50; ++i) {
            final List<WriteRequest> wr = new ArrayList<>();
            for (int j=0; j<10; ++j) {
                long v = System.currentTimeMillis();
                final Map<String, AttributeValue> m = new HashMap<>();
                m.put("A", AttributeValue.fromS(String.valueOf(j)));
                m.put("B", AttributeValue.fromS(String.valueOf(i)));
                m.put("X", AttributeValue.fromN(String.valueOf(v)));
                m.put("Y", AttributeValue.fromS(Long.toHexString(~v)));
                m.put("Z", AttributeValue.fromS("Баба Яга нумер " + ((10*(i+1)) + i + 1)));
                wr.add(WriteRequest.builder().putRequest(
                        PutRequest.builder().item(m).build()
                ).build());
            }
            client.batchWriteItem(BatchWriteItemRequest.builder()
                    .requestItems(Collections.singletonMap(tableName, wr))
                    .build());
        }
    }
    
    private TableDescription describeTable(DynamoDbClient client, String tableName) {
        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        try {
            return client.describeTable(dtr).table();
        } catch(ResourceNotFoundException rnfe) {
            return null;
        }
    }

    private TableDescription createTable(DynamoDbClient client, String tableName) {
        CreateTableRequest ctr = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(
                        AttributeDefinition.builder()
                                .attributeName("A").attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("B").attributeType(ScalarAttributeType.S)
                                .build())
                .keySchema(
                        KeySchemaElement.builder()
                                .attributeName("A").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder()
                                .attributeName("B").keyType(KeyType.RANGE).build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(1L).writeCapacityUnits(1L).build())
                .build();
        return client.createTable(ctr).tableDescription();
    }
    
}
