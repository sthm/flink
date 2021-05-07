package com.amazonaws.samples.flink.api.sink.writer;

import com.amazonaws.samples.flink.api.sink.committer.ApiSinkCommittable;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;


public abstract class ApiWriter<InputT, RequestT extends Serializable, ResponseT> implements SinkWriter<InputT, ApiSinkCommittable<ResponseT>, ApiWriterState<RequestT>> {

    private final Function<InputT, RequestT> elementToRequest;


    public ApiWriter(Function<InputT, RequestT> elementToRequest) {
        this.elementToRequest = elementToRequest;
    }

    /**
     * Function that converts a set of input elements into a batch put request. It also executes the batch request and
     * is responsible to re-queue all individual put requests that were not successfully persisted.
     */
    protected abstract CompletableFuture<ResponseT> submitRequestsToApi(List<RequestT> requests);


    /**
     * Buffer to hold request that should be persisted into the respective endpoint. Using a blocking deque so that
     * the sink can properly build backpressure.
     *
     * It seems more natural to buffer InputT, ie, the events that should be persisted, rather than RequestT.
     * However, in practice, the response of a failed API request call can make it very hard, if not impossible,
     * to reconstruct the original event. It is much easier, to just construct a new (retry) request from the response
     * and add that back to the queue for later retry.
     */
    private transient final LinkedBlockingDeque<RequestT> bufferedRequests = new LinkedBlockingDeque<>(1000);


    /**
     * Tracks all async batch put requests that have been executed since the last checkpoint. They may already have
     * been completed (successfully or unsuccessfully), which is tracked by submitRequests.
     *
     * To complete a checkpoint, we need to make sure that no requests are in flight, as they may fail which could
     * then lead to data loss.
     */
    private List<CompletableFuture<ResponseT>> inFlightRequests;



    @Override
    public void write(InputT element, Context context) throws IOException {
        bufferedRequests.offerLast(elementToRequest.apply(element));

        // if (flush condition) inFlightRequests.add(submitRequests(batch of requests));
    }

    public void requeueFailedRequest(RequestT request) {
        bufferedRequests.offerFirst(request);
    }

    @Override
    public List<ApiSinkCommittable<ResponseT>> prepareCommit(boolean flush) throws IOException {
        // if (flush) inFlightRequests.add(submitRequests(batch of requests));

        //block submission of new api calls during checkpointing, so that now new in-flight requests are created;

        ApiSinkCommittable<ResponseT> committable = new ApiSinkCommittable<>(inFlightRequests);
        inFlightRequests.clear();

        return Collections.singletonList(committable);
    }

    @Override
    public List<ApiWriterState<RequestT>> snapshotState() throws IOException {
        checkArgument(inFlightRequests.isEmpty());

        ApiWriterState<RequestT> state = new ApiWriterState<>(bufferedRequests);

        //enable submission of new api calls once checkpoint has been completed

        return Collections.singletonList(state);
    }

    @Override
    public void close() throws Exception {

    }
}
