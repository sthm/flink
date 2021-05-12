package com.amazonaws.samples.flink.api.sink.writer;

import com.amazonaws.samples.flink.api.sink.committer.ApiSinkCommittable;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.flink.util.Preconditions.checkArgument;


public abstract class ApiWriter<InputT, RequestT extends Serializable, ResponseT> implements SinkWriter<InputT, ApiSinkCommittable<ResponseT>, ApiWriterState<RequestT>> {

    static final Logger logger = LogManager.getLogger(ApiWriter.class);

    /**
     * This function specifies the mapping between elements of a stream to requests that can be sent to the API. The
     * mapping is provided by the user of a sink, not the sink creator.
     *
     * The requests contain all relevant information for the request to be persisted in the destination. Eg, for
     * Kinesis Data Streams, the request contains the payload and the partition key. The requests are buffered by the
     * AipWriter and sent to the API when the {@code submitRequestsToApi} method is invoked.
     */
    private final Function<InputT, RequestT> elementToRequest;


    /**
     * This method specifies how to persist buffered requests into the sink. It is provided by the sink creator.
     *
     * The method is invoked with a set of requests according to the buffering hints. The logic then needs to
     * create and execute the put against the API endpoint (ideally by batching together individual requests to increase
     * efficiency). The logic also needs to identify individual requests that were not persisted successfully and
     * resubmit them using the {@code requeueFailedRequest} method.
     *
     * The method returns a future that indicates, once completed, that all requests that are passed to the method have
     * either successfully completed or the requests have been re-queued.
     *
     * During checkpointing, the sink needs to ensure that there are no outstanding in-flight requests. Ie, that all
     * futures returned by this method are completed.
     *
     * @param requests a set of requests that should be sent to the API endpoint
     * @return a future that completes when all requests have been successfully put to the API or were requeued
     */
    protected abstract CompletableFuture<ResponseT> submitRequestsToApi(List<RequestT> requests);


    /**
     * Buffer to hold requests that should be persisted into the respective API endpoint. Using a blocking deque so that
     * the sink can properly build backpressure.
     *
     * A request contains all relevant details to make a request to the respective API. Eg, for Kinesis a request
     * contains the payload and partition key.
     *
     * It seems more natural to buffer InputT, ie, the events that should be persisted, rather than RequestT.
     * However, in practice, the response of a failed API request call can make it very hard, if not impossible,
     * to reconstruct the original event. It is much easier, to just construct a new (retry) request from the response
     * and add that back to the queue for later retry.
     */
    private transient final LinkedBlockingDeque<RequestT> bufferedRequests = new LinkedBlockingDeque<>();


    /**
     * Tracks all async put requests that have been executed since the last checkpoint. They may already have
     * been completed (successfully or unsuccessfully). Unsuccessful requests need to be handled by the logic in
     * {@code submitRequestsToApi}.
     *
     * To complete a checkpoint, we need to make sure that no requests are in flight, as they may fail which could
     * then lead to data loss.
     */
    private final LinkedBlockingDeque<CompletableFuture<ResponseT>> inFlightRequests = new LinkedBlockingDeque<>();



    @Override
    public void write(InputT element, Context context) throws IOException {
        bufferedRequests.offerLast(elementToRequest.apply(element));

        flush();  // just for testing
    }

    /**
     * Put a failed request back into the internal queue to retry later.
     */
    public void requeueFailedRequest(RequestT request) {
        bufferedRequests.offerFirst(request);
    }




    private static final int BATCH_SIZE = 100;       // just for testing purposes

    public ApiWriter(Function<InputT, RequestT> elementToRequest) {
        this.elementToRequest = elementToRequest;
    }




    /**
     * just for testing purposes
     */
    public void flush() {
        while (bufferedRequests.size() >= BATCH_SIZE) {
            ArrayList<RequestT> batch = new ArrayList<>();

            for (int i=0; i<BATCH_SIZE; i++) {
                RequestT request = bufferedRequests.remove();
                batch.add(request);
            }

            logger.info("submit requests for {} elements", batch.size());

            inFlightRequests.add(submitRequestsToApi(batch));
        }
    }


    /**
     * In flight requests may fail, they will be retried if the sink is still healthy.
     *
     * To not loose any requests, there cannot be any outstanding in-flight requests when a checkpoint/commit is in
     * process. To this end, all in-flight requests need to be completed and no new requests can be created during
     * a checkpoint.
     */
    @Override
    public List<ApiSinkCommittable<ResponseT>> prepareCommit(boolean flush) throws IOException {
        if (flush) {
            flush();
        }

        logger.info("Prepare commit. {} requests currently in flight.", inFlightRequests.size());

        //block submission of new api calls during checkpointing, so that now new in-flight requests are created;

        ApiSinkCommittable<ResponseT> committable = new ApiSinkCommittable<>(inFlightRequests);
        inFlightRequests.clear();

        return Collections.singletonList(committable);
    }

    /**
     * Buffered but not yet submitted requests are stored in the checkpoint/savepoint.
     */
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
