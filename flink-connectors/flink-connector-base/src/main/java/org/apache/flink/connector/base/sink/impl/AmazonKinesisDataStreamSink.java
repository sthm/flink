package org.apache.flink.connector.base.sink.impl;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class AmazonKinesisDataStreamSink<InputT> extends AsyncSinkBase<InputT, PutRecordsRequestEntry> {

    private static final Logger logger = LogManager.getLogger(AmazonKinesisDataStreamSink.class);

    private final String streamName;
    private final static KinesisAsyncClient client = KinesisAsyncClient.create();
    private final ElementConverter<InputT, PutRecordsRequestEntry> elementConverter;

    /**
     * Basic service properties and limits. Supported requests per sec, max batch size, max items per batch, etc.
     */
    private Object serviceProperties;

    private final ElementConverter<InputT, PutRecordsRequestEntry> SIMPLE_STRING_ELEMENT_CONVERTER =
            (element, context) -> PutRecordsRequestEntry
                .builder()
                .data(SdkBytes.fromUtf8String(element.toString()))
                .partitionKey(String.valueOf(element.hashCode()))
                .build();


    @Override
    public SinkWriter<InputT, Semaphore, Collection<PutRecordsRequestEntry>> createWriter(InitContext context, List<Collection<PutRecordsRequestEntry>> states) throws IOException {
        return new AmazonKinesisDataStreamWriter();
    }


    public AmazonKinesisDataStreamSink(String streamName, ElementConverter<InputT, PutRecordsRequestEntry> elementConverter, KinesisAsyncClient client) {
        this.streamName = streamName;
        this.elementConverter = elementConverter;
//        this.client = client;

        // verify that user supplied buffering strategy respects service specific limits
    }

    public AmazonKinesisDataStreamSink(String streamName) {
        this.streamName = streamName;
        this.elementConverter = SIMPLE_STRING_ELEMENT_CONVERTER;
//        this.client = KinesisAsyncClient.create();
    }


    private class AmazonKinesisDataStreamWriter extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {

        public AmazonKinesisDataStreamWriter() {
            super(elementConverter);
        }

        @Override
        protected void submitRequestEntries(List<PutRecordsRequestEntry> requestEntries, ResultFuture<?> requestResult) {
            // create a batch request
            PutRecordsRequest batchRequest = PutRecordsRequest
                    .builder()
                    .records(requestEntries)
                    .streamName(streamName)
                    .build();


            // call api with batch request
            CompletableFuture<PutRecordsResponse> future = client.putRecords(batchRequest);


            // re-queue elements of failed requests
            future.whenComplete((response, err) -> {
                    if (err != null) {
                        requestEntries.forEach(this::requeueFailedRequestEntry);

                        requestResult.completeExceptionally(err);

                        return;
                    }

                    if (response.failedRecordCount() > 0) {
                        logger.warn("Re-queueing {} messages", response.failedRecordCount());

                        List<PutRecordsResultEntry> records = response.records();

                        for (int i = 0; i < records.size(); i++) {
                            if (records.get(i).errorCode() != null) {
                                requeueFailedRequestEntry(requestEntries.get(i));
                            }
                        }
                    }

                    requestResult.complete(Collections.emptyList());
                });
        }
    }
}