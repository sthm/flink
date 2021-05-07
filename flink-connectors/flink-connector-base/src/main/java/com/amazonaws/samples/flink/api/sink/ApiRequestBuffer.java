package com.amazonaws.samples.flink.api.sink;

/**
 * The producer is responsible for buffering/batching requests according to buffering hints from the user.
 * It triggers batch puts and tracks in flight request to ensure that all requests are
 * eventually persisted.
 */
public abstract class ApiRequestBuffer<RequestT, ResponseT> {


    /**
     * Configuration from the end user, such as, buffering hints, etc.
     */
    private Object producerConfiguration;

    /**
     * take n requests from bufferedRequests and create a list of requests
     * invoke submitRequests function, that makes an async call against the AWS service
     * add future to inFlightRequests so that it can be tracked if there are outstanding requests
     */
    private void flush() {
        // inFlightRequests.add(submitRequests(batch of requests));
    }

}
