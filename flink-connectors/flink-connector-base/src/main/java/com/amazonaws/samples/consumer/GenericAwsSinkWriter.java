package com.amazonaws.samples.consumer;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

public abstract class GenericAwsSinkWriter<InputT, ClientT, RequestT, ResponseT> implements SinkWriter<InputT, Void, Void> {

    private int batchSize = 10;

    protected transient ClientT client;
    protected  transient LinkedBlockingDeque<RequestT> elements;

    public abstract RequestT queueElement(InputT element) throws IOException;


    public void queueRequest(RequestT request) {
        elements.add(request);
        //flush?
    }

    public abstract CompletableFuture<ResponseT> submitRequests(List<RequestT> requests);

    @Override
    public void write(InputT element, Context context) throws IOException {
        elements.add(queueElement(element));

        if (elements.size() > batchSize) { //or timeout, etc
            sendRequests();
        }
    }

    private void sendRequests() {
        int size = Math.min(elements.size(), batchSize);

        ArrayList<RequestT> list = new ArrayList<>(size);

        // iterate to create multiple requests

        try {
            for (int i=0; i<size; i++) {
                list.add(elements.take());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        submitRequests(list);

        // on checkpoint, collect all futures and join them CompletableFuture.allOf(); cleanup list on checkpoint
    }



    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException {
        return null;
    }

    @Override
    public List<Void> snapshotState() throws IOException {
        do {
            sendRequests();
            // wait on all futures to complete
        } while (!elements.isEmpty());

        return null;
    }

    @Override
    public void close() throws Exception {

    }





    /*
    public void writeKinesis(InputT element) {
        KinesisAsyncClient client = KinesisAsyncClient.create();

        PutRecordsRequestEntry request = PutRecordsRequestEntry
                .builder()
                .data(SdkBytes.fromUtf8String("test"))
                .build();

        PutRecordsRequest putRecords = PutRecordsRequest
                .builder()
                .records(request)
                .build();

        client.putRecords(putRecords);

        PutRecordsResponse response = null;


        // match index of failed record to PutRecordsRequestEntry
        for (int i=0; i < response.records().size(); i++) {
            if (response.records().get(i).errorCode() != null) {
                //re-add back to queue
                putRecords.records().get(i);
            }
        }

        boolean successful = response.sdkHttpResponse().isSuccessful() && response.failedRecordCount()==0;
    }


    public void writeNeptune(InputT element) {
        NeptuneAsyncClient client = NeptuneAsyncClient.create();
    }


    public void writeDynamo(InputT element) {
        DynamoDbAsyncClient client = DynamoDbAsyncClient.create();

        PutItemRequest request = PutItemRequest
                .builder()
                .tableName("bla")
                .item(null)
                .build();

        client.putItem(request);

//        PutItemResponse response = null;

        BatchWriteItemResponse response = null;

        boolean successful = response.sdkHttpResponse().isSuccessful() && response.hasUnprocessedItems();

        response.unprocessedItems();
    }


    public void writeGeneric(InputT element) {

    }
    */

}
