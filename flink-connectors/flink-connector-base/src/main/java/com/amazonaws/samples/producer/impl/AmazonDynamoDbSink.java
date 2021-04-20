package com.amazonaws.samples.producer.impl;

import com.amazonaws.samples.producer.GenericAwsProducer;
import com.amazonaws.samples.producer.GenericAwsSink;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AmazonDynamoDbSink<InputT extends Map<String, AttributeValue>> extends GenericAwsSink<InputT, DynamoDbAsyncClient, WriteRequest, BatchWriteItemResponse> {

    public AmazonDynamoDbSink() {
        this.producer = new AmazonDynamoDbProducer();
    }

    private class AmazonDynamoDbProducer extends GenericAwsProducer<InputT, DynamoDbAsyncClient, WriteRequest, BatchWriteItemResponse> {
        @Override
        public WriteRequest queueElement(InputT element) {
            PutRequest putRequest = PutRequest
                    .builder()
                    .item(element)
                    .build();

            return WriteRequest
                    .builder()
                    .putRequest(putRequest)
                    .build();
        }

        @Override
        public CompletableFuture<BatchWriteItemResponse> submitRequests(List<WriteRequest> requests) {
            Map<String, List<WriteRequest>> items = new HashMap<>();
            items.put("table-name", requests);

            BatchWriteItemRequest request = BatchWriteItemRequest
                    .builder()
                    .requestItems(items)
                    .build();

            CompletableFuture<BatchWriteItemResponse> future = client.batchWriteItem(request);

            future.whenComplete((response, err) -> {
                // re-queue all requests that failed
                response.unprocessedItems().get("table-name").forEach(this::queueRequest);

                // handle errors of the entire request...
            });

            return future;
        }
    }
}
