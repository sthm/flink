package com.amazonaws.samples.producer.Implementations;

import com.amazonaws.samples.producer.GenericAwsSinkWriter;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DynamoDbSinkWriter<InputT extends Map<String, AttributeValue>> extends GenericAwsSinkWriter<InputT, DynamoDbAsyncClient, WriteRequest, BatchWriteItemResponse> {

    public DynamoDbSinkWriter() {
       super(DynamoDbAsyncClient.builder().build());
    }

    @Override
    public WriteRequest queueElement(InputT element) throws IOException {
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
        Map<String,List<WriteRequest>> items = new HashMap<>();
        items.put("table-name", requests);

        BatchWriteItemRequest request = BatchWriteItemRequest
                .builder()
                .requestItems(items)
                .build();

        CompletableFuture<BatchWriteItemResponse> future = client.batchWriteItem(request);

        future.whenComplete((response, err) -> {
            // re-queue all requests that failed
            response.unprocessedItems().get("table-name").forEach(this::queueRequest);
        });

        return future;
    }
}
