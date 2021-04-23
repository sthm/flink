package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.GenericApiProducer;
import com.amazonaws.samples.flink.api.sink.GenericApiSink;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AmazonKinesisSink<InputT> extends GenericApiSink<InputT, KinesisAsyncClient, PutRecordsRequestEntry, PutRecordsResponse> {

    public AmazonKinesisSink() {
        this.producer = new AmazonKinesisProducer();

        // initialize service specific buffering hints (maybe static?)
        // set user specific buffering hints & config through constructor
    }


    private class AmazonKinesisProducer extends GenericApiProducer<InputT, KinesisAsyncClient, PutRecordsRequestEntry, PutRecordsResponse> {
        @Override
        public PutRecordsRequestEntry convertToRequest(InputT element) {
            return PutRecordsRequestEntry
                    .builder()
                    .data(SdkBytes.fromUtf8String(element.toString()))
                    .partitionKey(element.toString())
                    .build();
        }

        @Override
        public CompletableFuture<PutRecordsResponse> submitBatchRequestToApi(List<PutRecordsRequestEntry> requests) {
            PutRecordsRequest request = PutRecordsRequest
                    .builder()
                    .records(requests)
                    .build();

            CompletableFuture<PutRecordsResponse> future = client.putRecords(request);

            future.whenComplete((response, err) -> {
                // re-queue all requests that failed, can be skipped if people don't care for at-least once semantics
                if (response.failedRecordCount() > 0) {
                    for (int i=0; i<response.failedRecordCount(); i++) {
                        if (response.records().get(i).errorCode() != null) {
                            queueRequest(requests.get(i));
                        }
                    }
                }

                //handle errors of the entire request...
            });

            return future;
        }
    }
}