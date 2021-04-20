package com.amazonaws.samples.producer.impl;

import com.amazonaws.samples.producer.GenericAwsProducer;
import com.amazonaws.samples.producer.GenericAwsSink;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AmazonKinesisSink<InputT> extends GenericAwsSink<InputT, KinesisAsyncClient, PutRecordsRequestEntry, PutRecordsResponse> {

    public AmazonKinesisSink() {
        this.producer = new AmazonKinesisProducer();
    }


    private class AmazonKinesisProducer<InputT> extends GenericAwsProducer<InputT, KinesisAsyncClient, PutRecordsRequestEntry, PutRecordsResponse> {
        @Override
        public software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry queueElement(InputT element) {
            return software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
                    .builder()
                    .data(SdkBytes.fromUtf8String(element.toString()))
                    .partitionKey(element.toString())
                    .build();
        }

        @Override
        public CompletableFuture<software.amazon.awssdk.services.kinesis.model.PutRecordsResponse> submitRequests(List<software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry> requests) {
            PutRecordsRequest request = PutRecordsRequest
                    .builder()
                    .records(requests)
                    .build();

            CompletableFuture<software.amazon.awssdk.services.kinesis.model.PutRecordsResponse> future = client.putRecords(request);

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