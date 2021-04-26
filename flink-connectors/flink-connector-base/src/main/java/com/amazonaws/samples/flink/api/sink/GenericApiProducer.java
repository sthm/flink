package com.amazonaws.samples.flink.api.sink;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;


/**
 * The producer is responsible for buffering/batching requests according to buffering hints from the user.
 * It triggers batch puts and tracks in flight request to ensure that all requests are
 * eventually persisted.
 */
public abstract class GenericApiProducer<RequestT extends Serializable, ResponseT> {

    /**
     * Function that converts a set of input elements into a batch put request. It also executes the batch request and
     * is responsible to re-queue all individual put requests that were not successfully persisted.
     */
    public abstract CompletableFuture<ResponseT> submitRequestToApi(List<RequestT> elements);


    /**
     * Basic service properties and limits. Supported requests per sec, batch size, etc.
     */
    private Object ServiceProperties;

    /**
     * Configuration from the end user, such as, buffering hints, credentials procider, etc.
     */
    private Object ProducerConfiguration;

    /**
     * Buffer to hold request that should be persisted into the respective endpoint. Using a blocking deque so that
     * the sink can properly build backpressure.
     */
    private transient LinkedBlockingDeque<RequestT> bufferedRequests = new LinkedBlockingDeque<>(1000);


    /**
     * Buffers a request that should be sent to the API. New requests are stored at the end of the buffer.
     */
    public void queueRequest(RequestT request) {
        bufferedRequests.offerLast(request);
    }

    /**
     * Buffers a request that was already sent, but failed. Failed requests are stored at the beginning of the buffer
     * to minimize latency.
     */
    public void requeueFailedRequest(RequestT request) {
        bufferedRequests.offerFirst(request);
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
     * Make sure that all inFlight requests are completed and no new requests can be created.
     */
    public void initiateCheckpoint() {
        //block calls to flush(); Promise.all(inFlightRequests); inFlightRequests.clear();
    }

    /**
     * Allow new requests once the checkpoint completed
     */
    public void completeCheckpoint() {
        //unblock call to flush()
    }

    // TODO: for batch apps: flushSync on job completion?
}
