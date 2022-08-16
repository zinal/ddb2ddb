package com.yandex.ydb.tools.ddb2ddb;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 *
 * @author mzinal
 */
public class Helpers {

    public static TableDescription describeTable(DynamoDbClient client, String tableName) {
        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        try {
            return client.describeTable(dtr).table();
        } catch(ResourceNotFoundException rnfe) {
            return null;
        }
    }

}
