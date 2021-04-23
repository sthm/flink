package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.GenericApiProducer;
import com.amazonaws.samples.flink.api.sink.GenericApiSink;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AmazonDynamoDbSink<InputT extends Map<String, AttributeValue>> extends GenericApiSink<InputT, DynamoDbAsyncClient, WriteRequest, BatchWriteItemResponse> {

    public AmazonDynamoDbSink() {
        this.producer = new AmazonDynamoDbProducer();

        // initialize service specific buffering hints (maybe static?)
        // set user specific buffering hints & config through constructor
    }

    private class AmazonDynamoDbProducer extends GenericApiProducer<InputT, DynamoDbAsyncClient, WriteRequest, BatchWriteItemResponse> {
        @Override
        public WriteRequest convertToRequest(InputT element) {
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
        public CompletableFuture<BatchWriteItemResponse> submitBatchRequestToApi(List<WriteRequest> requests) {
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
