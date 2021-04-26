package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.GenericApiProducer;
import com.amazonaws.samples.flink.api.sink.GenericApiSink;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonKinesisDataFirehoseSink<InputT> extends GenericApiSink<InputT, Record, PutRecordBatchResponse> {

    private final String deliveryStreamName;
    private final FirehoseAsyncClient client;

    public AmazonKinesisDataFirehoseSink(String deliveryStreamName, Function<InputT, Record> elementToRequest, FirehoseAsyncClient client) {
        this.deliveryStreamName = deliveryStreamName;
        this.elementToRequest = elementToRequest;
        this.client = client;

        this.producer = new AmazonKinesisDataFirehoseProducer();
    }

    private class AmazonKinesisDataFirehoseProducer extends GenericApiProducer<Record, PutRecordBatchResponse> {
        @Override
        public CompletableFuture<PutRecordBatchResponse> submitRequestToApi(List<Record> requests) {

            PutRecordBatchRequest batchRequest = PutRecordBatchRequest
                    .builder()
                    .records(requests)
                    .deliveryStreamName(deliveryStreamName)
                    .build();

            CompletableFuture<PutRecordBatchResponse> future = client.putRecordBatch(batchRequest);

            future.whenComplete((response, err) -> {
                if (response.failedPutCount() > 0) {
                    List<PutRecordBatchResponseEntry> records = response.requestResponses();

                    for (int i = 0; i < records.size(); i++) {
                        if (records.get(i).errorCode() != null) {
                            requeueFailedRequest(requests.get(i));
                        }
                    }
                }
            });

            return future;
        }
    }
}