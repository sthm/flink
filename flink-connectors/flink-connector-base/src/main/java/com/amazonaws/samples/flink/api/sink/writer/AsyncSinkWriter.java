package com.amazonaws.samples.flink.api.sink.writer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;


public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable> implements SinkWriter<InputT, Collection<CompletableFuture<?>>, Collection<RequestEntryT>> {

    static final Logger logger = LogManager.getLogger(AsyncSinkWriter.class);

    /**
     * This function specifies the mapping between elements of a stream to
     * request entries that can be sent to the API. The mapping is provided by
     * the end-user of a sink, not the sink creator.
     * <p>
     * The request entries contain all relevant information required to create
     * and sent the actual request. Eg, for Kinesis Data Streams, the request
     * contains the payload and the partition key. The requests are buffered by
     * the AipWriter and sent to the API when the {@code submitRequestsToApi}
     * method is invoked.
     */
    private final Function<InputT, RequestEntryT> elementToRequest;


    /**
     * This method specifies how to persist buffered request entries into the
     * sink. It is implemented when a new endpoint or service should be
     * supported.
     * <p>
     * The method is invoked with a set of request entries according to the
     * buffering hints. The logic then needs to create and execute the request
     * against the API endpoint (ideally by batching together multiple request
     * entries to increase efficiency). The logic also needs to identify
     * individual request entries that were not persisted successfully and
     * resubmit them using the {@code requeueFailedRequest} method.
     * <p>
     * The method returns a future that indicates, once completed, that all
     * request entries that have been passed to the method on invocation have
     * either successfully completed or the request entries have been
     * re-queued.
     * <p>
     * During checkpointing, the sink needs to ensure that there are no
     * outstanding in-flight requests. Ie, that all futures returned by this
     * method are completed.
     *
     * @param requestEntries a set of requests that should be sent to the API
     *                       endpoint
     * @return a future that completes when all request entries have been
     * successfully persisted to the API or were re-queued
     */
    protected abstract CompletableFuture<?> submitRequestEntries(List<RequestEntryT> requestEntries);


    /**
     * Buffer to hold request entries that should be persisted into the
     * respective endpoint.
     * <p>
     * A request entry contain all relevant details to make a request to the
     * respective API. Eg, for Kinesis a request entry contains the payload and
     * partition key.
     * <p>
     * It seems more natural to buffer InputT, ie, the events that should be
     * persisted, rather than RequestEntryT. However, in practice, the response
     * of a failed request call can make it very hard, if not impossible, to
     * reconstruct the original event. It is much easier, to just construct a
     * new (retry) request entry from the response and add that back to the
     * queue for later retry.
     */
    private final Queue<RequestEntryT> bufferedRequests = new ConcurrentLinkedQueue<>();


    /**
     * Tracks all async requests that have been executed since the last
     * checkpoint. Requests that already completed (successfully or
     * unsuccessfully) are automatically removed from the queue. Any request
     * entry that was not successfully persisted need to be handled and retried
     * by the logic in {@code submitRequestsToApi}.
     * <p>
     * There is a limit on the number of concurrent (async) requests that can be
     * handled by the client library.
     * <p>
     * To complete a checkpoint, we need to make sure that no requests are in
     * flight, as they may fail, which could then lead to data loss.
     */
    private Queue<CompletableFuture<?>> inFlightRequests = new ConcurrentLinkedQueue<>();



    @Override
    public void write(InputT element, Context context) throws IOException {
        bufferedRequests.add(elementToRequest.apply(element));

        flush();  // just for testing
    }

    /**
     * A request or single request entries of a request may fail, eg, because of
     * network issues or service side throttling. All request entries that
     * failed with transient failures need to be re-queued with this method so
     * that aren't lost and can be retried later.
     * <p>
     * Request entries that are causing the same error in a reproducible manner,
     * eg, ill-formed request entries, must not be re-queued but the error needs
     * to be handled in the logic of {@code submitRequestEntries}. Otherwise
     * these request entries will be retried indefinitely, always causing the
     * same error.
     */
    protected void requeueFailedRequestEntry(RequestEntryT requestEntry) {
        bufferedRequests.add(requestEntry);
    }




    private static final int BATCH_SIZE = 100;       // just for testing purposes

    public AsyncSinkWriter(Function<InputT, RequestEntryT> elementToRequest) {
        this.elementToRequest = elementToRequest;
    }




    /**
     * just for testing purposes
     */
    public void flush() {
        while (bufferedRequests.size() >= BATCH_SIZE) {
            ArrayList<RequestEntryT> batch = new ArrayList<>();

            for (int i=0; i<BATCH_SIZE; i++) {
                RequestEntryT request = bufferedRequests.remove();
                batch.add(request);
            }

            logger.info("submit requests for {} elements", batch.size());

            //TODO: add another future that removes the request from the queue once it completed
            inFlightRequests.add(submitRequestEntries(batch));
        }
    }


    /**
     * In flight requests may fail, but they will be retried if the sink is
     * still healthy.
     * <p>
     * To not lose any requests, there cannot be any outstanding in-flight
     * requests when a checkpoint/commit is in process. To this end, all
     * in-flight requests need to be completed and no new requests can be
     * created until a checkpoint/savepoint completed.
     */
    @Override
    public List<Collection<CompletableFuture<?>>> prepareCommit(boolean flush) throws IOException {
        if (flush) {
            flush();
        }

        logger.info("Prepare commit. {} requests currently in flight.", inFlightRequests.size());

        //TODO: block submission of new api calls during checkpointing, so that now new in-flight requests are created;

        // reuse current inFlightRequests as commitable and create empty queue to avoid copy and clearing
        List<Collection<CompletableFuture<?>>> committable = Collections.singletonList(inFlightRequests);

        // during a checkpoint/savepoint, no new requests can be created, so it's save to create a new queue
        inFlightRequests = new ConcurrentLinkedQueue<>();

        return committable;
    }

    /**
     * All in-flight requests have been completed, but there may still be
     * request entries in the internal buffer that are yet to be sent to the
     * endpoint. These request entries are stored in the snapshot state so that
     * they don't get lost in case of a failure/restart of the application.
     */
    @Override
    public List<Collection<RequestEntryT>> snapshotState() throws IOException {
        checkArgument(inFlightRequests.isEmpty());

        //TODO: enable submission of new api calls once checkpoint has been completed

        return Collections.singletonList(bufferedRequests);
    }



    @Override
    public void close() throws Exception {

    }
}
