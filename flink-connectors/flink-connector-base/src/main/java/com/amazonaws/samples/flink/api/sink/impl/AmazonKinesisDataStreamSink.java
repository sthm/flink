package com.amazonaws.samples.flink.api.sink.impl;

import com.amazonaws.samples.flink.api.sink.AsyncSink;
import com.amazonaws.samples.flink.api.sink.writer.AsyncSinkWriter;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AmazonKinesisDataStreamSink<InputT> extends AsyncSink<InputT, PutRecordsRequestEntry> {

    private static final Logger logger = LogManager.getLogger(AmazonKinesisDataStreamSink.class);

    private final String streamName;
    private final static KinesisAsyncClient client = KinesisAsyncClient.create();
//    private final static Function<InputT, PutRecordsRequestEntry> elementToRequest;

    /**
     * Basic service properties and limits. Supported requests per sec, max batch size, max items per batch, etc.
     */
    private Object serviceProperties;

    @Override
    public SinkWriter<InputT, Collection<CompletableFuture<?>>, Collection<PutRecordsRequestEntry>> createWriter(InitContext context, List<Collection<PutRecordsRequestEntry>> states) throws IOException {
        return new AmazonKinesisDataStreamWriter();
    }


    public AmazonKinesisDataStreamSink(String streamName, Function<InputT, PutRecordsRequestEntry> elementToRequest, KinesisAsyncClient client) {
        this.streamName = streamName;
//        this.elementToRequest = elementToRequest;
//        this.client = client;

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


    private class AmazonKinesisDataStreamWriter extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {

        public AmazonKinesisDataStreamWriter() {
            super(element ->
                    PutRecordsRequestEntry
                            .builder()
                            .data(SdkBytes.fromUtf8String(element.toString()))
                            .partitionKey(String.valueOf(element.hashCode()))
                            .build());
        }

        @Override
        protected CompletableFuture<?> submitRequestEntries(List<PutRecordsRequestEntry> requestEntries) {
            // create a batch request
            PutRecordsRequest batchRequest = PutRecordsRequest
                    .builder()
                    .records(requestEntries)
                    .streamName(streamName)
                    .build();


            // call api with batch request
            CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);


            // re-queue elements of failed requests
            CompletableFuture<PutRecordsResponse> handleResponse = future
                .whenComplete((response, err) -> {
                    if (err != null) {
                        logger.error(err);

                        return;
                    }

                    if (response.failedRecordCount() > 0) {
                        List<PutRecordsResultEntry> records = response.records();

                        for (int i = 0; i < records.size(); i++) {
                            if (records.get(i).errorCode() != null) {
                                requeueFailedRequestEntry(requestEntries.get(i));

                                logger.warn("Retrying message: {}", requestEntries.get(i));
                            }
                        }
                    }

                    //TODO: handle errors of the entire request...
                });


            // return future to track completion of async request
            return handleResponse;
        }

    }
}