package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.GenericApiProducer;
import com.amazonaws.samples.flink.api.sink.GenericApiSink;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonDynamoDbSink<InputT> extends GenericApiSink<InputT, WriteRequest, BatchWriteItemResponse> {

    private final String tableName;
    private DynamoDbAsyncClient client = DynamoDbAsyncClient.create();


    public AmazonDynamoDbSink(String tableName, Function<InputT, WriteRequest> elementToRequest) {
        this.tableName = tableName;
        this.elementToRequest = elementToRequest;

        this.producer = new AmazonDynamoDbProducer();

        // initialize service specific buffering hints (maybe static?)
        // set user specific buffering hints & config through constructor
    }


    private class AmazonDynamoDbProducer extends GenericApiProducer<WriteRequest, BatchWriteItemResponse> {
        @Override
        public CompletableFuture<BatchWriteItemResponse> submitRequestToApi(List<WriteRequest> elements) {

            Map<String, List<WriteRequest>> items = new HashMap<>();
            items.put(tableName, elements);

            BatchWriteItemRequest request = BatchWriteItemRequest
                    .builder()
                    .requestItems(items)
                    .build();

            CompletableFuture<BatchWriteItemResponse> future = client.batchWriteItem(request);

            future.whenComplete((response, err) -> {
                // re-queue all requests that failed
                response.unprocessedItems().get(tableName).forEach(this::requeueFailedRequest);

                // handle errors of the entire request...
            });

            return future;
        }
    }


    private static Function<Map<String, AttributeValue>, WriteRequest> DEFAULT_ELEMENT_TO_REQUEST = element -> {
        PutRequest putRequest = PutRequest
                .builder()
                .item(element)
                .build();

        return WriteRequest
                .builder()
                .putRequest(putRequest)
                .build();
    };
}
