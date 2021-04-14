package com.amazonaws.samples.producer;

import org.apache.flink.api.connector.sink.SinkWriter;
import software.amazon.awssdk.core.SdkClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class GenericAwsSinkWriter<InputT, ClientT extends SdkClient, RequestT, ResponseT> implements SinkWriter<InputT, Void, Void> {

    private int batchSize = 10;

    protected transient ClientT client;
    protected  transient LinkedBlockingDeque<RequestT> elements;

    /* convert a single event into a singe request that can be executed */
    public abstract RequestT queueElement(InputT element) throws IOException;

    /* receive a list of single requests (according to buffering and max batch size), create and execute a batch request for the service */
    public abstract CompletableFuture<ResponseT> submitRequests(List<RequestT> requests);



    public GenericAwsSinkWriter(ClientT client) {
        this.client = client;
    }

    public void queueRequest(RequestT request) {
        elements.add(request);
        //flush?
    }



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
        client.close();
    }

}
