package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.ApiRequestBuffer;
import com.amazonaws.samples.flink.api.sink.GenericApiSink;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonKinesisDataStreamSink<InputT> extends GenericApiSink<InputT, PutRecordsRequestEntry, PutRecordsResponse> {

    private final String streamName;
    private final KinesisAsyncClient client;

    /**
     * Basic service properties and limits. Supported requests per sec, max batch size, max items per batch, etc.
     */
    private Object serviceProperties;

    public AmazonKinesisDataStreamSink(String streamName, Function<InputT, PutRecordsRequestEntry> elementToRequest, KinesisAsyncClient client) {
        this.streamName = streamName;
        this.elementToRequest = elementToRequest;
        this.client = client;

        this.producer = new AmazonKinesisDataStreamRequestBuffer();

        // verify that user supplied buffering strategy respects service specific limits
    }


    private class AmazonKinesisDataStreamRequestBuffer extends ApiRequestBuffer<PutRecordsRequestEntry, PutRecordsResponse> {
        @Override
        public CompletableFuture<PutRecordsResponse> submitRequestsToApi(List<PutRecordsRequestEntry> requests) {
            // create a batch requests
            PutRecordsRequest batchRequest = PutRecordsRequest
                    .builder()
                    .records(requests)
                    .streamName(streamName)
                    .build();


            // call api with batch request
            CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);


            // re-queue elements of failed requests
            future.whenComplete((response, err) -> {
                if (response.failedRecordCount() > 0) {
                    List<PutRecordsResultEntry> records = response.records();

                    for (int i=0; i<records.size(); i++) {
                        if (records.get(i).errorCode() != null) {
                            requeueFailedRequest(requests.get(i));
                        }
                    }
                }

                //handle errors of the entire request...
            });


            // return future to track completion of async request
            return future;
        }
    }


    private final Function<InputT, PutRecordsRequestEntry> DEFAULT_ELEMENT_TO_REQUEST = element ->
            PutRecordsRequestEntry
                    .builder()
                    .data(SdkBytes.fromUtf8String(element.toString()))
                    .partitionKey(element.toString())
                    .build();
}