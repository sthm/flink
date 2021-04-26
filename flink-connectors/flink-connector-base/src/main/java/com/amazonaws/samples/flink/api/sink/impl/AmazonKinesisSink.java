package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.GenericApiProducer;
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

public class AmazonKinesisSink<InputT> extends GenericApiSink<InputT, PutRecordsRequestEntry, PutRecordsResponse> {

    private final String streamName;
    private final KinesisAsyncClient client = KinesisAsyncClient.create();

    public AmazonKinesisSink(String streamName, Function<InputT, PutRecordsRequestEntry> elementToRequest) {
        this.streamName = streamName;
        this.elementToRequest = elementToRequest;

        this.producer = new AmazonKinesisProducer();

        // initialize service specific buffering hints (maybe static?)
        // set user specific buffering hints & config through constructor
    }


    private class AmazonKinesisProducer extends GenericApiProducer<PutRecordsRequestEntry, PutRecordsResponse> {
        @Override
        public CompletableFuture<PutRecordsResponse> submitRequestToApi(List<PutRecordsRequestEntry> requests) {
            // create a batch requests
            PutRecordsRequest request = PutRecordsRequest
                    .builder()
                    .records(requests)
                    .streamName(streamName)
                    .build();


            // call api with batch request
            CompletableFuture<PutRecordsResponse> future = client.putRecords(request);


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