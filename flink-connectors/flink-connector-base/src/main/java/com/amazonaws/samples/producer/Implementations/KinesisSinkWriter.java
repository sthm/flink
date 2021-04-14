package com.amazonaws.samples.producer.Implementations;

import com.amazonaws.samples.producer.GenericAwsSinkWriter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KinesisSinkWriter<InputT> extends GenericAwsSinkWriter<InputT, KinesisAsyncClient, PutRecordsRequestEntry, PutRecordsResponse> {

    public KinesisSinkWriter() {
        // supply buffering hints, etc...

        super(KinesisAsyncClient.builder().build());
    }

    @Override
    public PutRecordsRequestEntry queueElement(InputT element) {
        return PutRecordsRequestEntry
                .builder()
                .data(SdkBytes.fromUtf8String(element.toString()))
                .partitionKey(element.toString())
                .build();
    }

    @Override
    public CompletableFuture<PutRecordsResponse> submitRequests(List<PutRecordsRequestEntry> requests) {
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
        });

        return future;
    }

}