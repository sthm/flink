package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.ApiSink;
import com.amazonaws.samples.flink.api.sink.committer.ApiSinkCommittable;
import com.amazonaws.samples.flink.api.sink.writer.ApiWriter;
import com.amazonaws.samples.flink.api.sink.writer.ApiWriterState;
import org.apache.flink.api.connector.sink.SinkWriter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonKinesisDataStreamSink<InputT> extends ApiSink<InputT, PutRecordsRequestEntry, PutRecordsResponse> {

    private final String streamName;
    private final KinesisAsyncClient client;
    private final Function<InputT, PutRecordsRequestEntry> elementToRequest;

    /**
     * Basic service properties and limits. Supported requests per sec, max batch size, max items per batch, etc.
     */
    private Object serviceProperties;

    @Override
    public SinkWriter<InputT, ApiSinkCommittable<PutRecordsResponse>, ApiWriterState<PutRecordsRequestEntry>> createWriter(InitContext context, List<ApiWriterState<PutRecordsRequestEntry>> states) throws IOException {
        return new AmazonKinesisDataStreamWriter();
    }


    public AmazonKinesisDataStreamSink(String streamName, Function<InputT, PutRecordsRequestEntry> elementToRequest, KinesisAsyncClient client) {
        this.streamName = streamName;
        this.elementToRequest = elementToRequest;
        this.client = client;

        // verify that user supplied buffering strategy respects service specific limits
    }

    public AmazonKinesisDataStreamSink(String streamName) {
        this(
                streamName,
                element ->
                        PutRecordsRequestEntry
                                .builder()
                                .data(SdkBytes.fromUtf8String(element.toString()))
                                .partitionKey(element.toString())
                                .build(),
                KinesisAsyncClient.create()
        );
    }


    private class AmazonKinesisDataStreamWriter extends ApiWriter<InputT, PutRecordsRequestEntry, PutRecordsResponse> {

        public AmazonKinesisDataStreamWriter() {
            super(elementToRequest);
        }

        @Override
        protected CompletableFuture<PutRecordsResponse> submitRequestsToApi(List<PutRecordsRequestEntry> requests) {
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

                    for (int i = 0; i < records.size(); i++) {
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
}