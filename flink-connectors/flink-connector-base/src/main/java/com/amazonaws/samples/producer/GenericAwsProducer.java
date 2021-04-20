package com.amazonaws.samples.producer;

import software.amazon.awssdk.core.SdkClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * The producer is responsible for buffering/batching requests according to buffering hints from the user.
 * It's also responsible for triggering batch puts and tracking in flight request.
 */
public abstract class GenericAwsProducer<InputT, ClientT extends SdkClient, RequestT, ResponseT> {

    /**
     * The AWS sdk client that communicates with the respective sink.
     */
    protected transient ClientT client;

    /**
     * Basic service properties and limits.
     */
    private Object ServiceProperties;

    /**
     * Configuration from the end user, such as, buffering hints, credentials procider, etc.
     */
    private Object ProducerConfiguration;

    /**
     * Buffer to hold request that should be persisted into the respective sink.
     */
    private transient LinkedBlockingDeque<RequestT> bufferedRequests;



    /**
     * Function that converts a set of put requests into a batch put request. It also executes the batch request and
     * is responsible to re-queue all individual put requests that were not successfully persisted.
     */
    public abstract CompletableFuture<ResponseT> submitRequests(List<RequestT> requests);

    /**
     * Takes an event from an internal (keyed) stream and converts it into an appropriate put request that can be
     * issued through ClientT.
     */
    public abstract RequestT queueElement(InputT element);

    /**
     * Helper method to re-queue requests that have failed.
     */
    public void queueRequest(RequestT request) {
        bufferedRequests.add(request);
    }



    /**
     * Tracks all async batch put requests that have been executed since the last checkpoint. They may already have
     * been completed (successfully or unsuccessfully), which is tracked by submitRequests.
     *
     * To complete a checkpoint, we need to make sure that no requests are in flight, as they may fail which could
     * then lead to data loss.
     */
    private List<CompletableFuture<ResponseT>> inFlightRequests;

    /**
     * take n requests from bufferedRequests and create a list of requests
     * invoke submitRequests function, that makes an async call against the AWS service
     * add future to inFlightRequests so that it can be tracked if there are outstanding requests
     */
    private void flush() {
        // inFlightRequests.add(submitRequests(batch of requests));
    }


    /**
     * invoke flush operation and wait until all futures in inFlightRequests have completed
     * -> block creation of any new requests during that time
     */
    private void flushSync() {
        //flush(); Promise.all(inFlightRequests); inFlightRequests.clear(); iterate
    }
}
