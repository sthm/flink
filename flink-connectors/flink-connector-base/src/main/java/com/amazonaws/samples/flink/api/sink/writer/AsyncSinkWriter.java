package com.amazonaws.samples.flink.api.sink.writer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;


public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable> implements SinkWriter<InputT, Collection<CompletableFuture<?>>, Collection<RequestEntryT>> {

    static final Logger logger = LogManager.getLogger(AsyncSinkWriter.class);


    /**
     * The ElementConverter provides a  mapping between for the elements of a
     * stream to request entries that can be sent to the destination.
     * <p>
     * The resulting request * entry is buffered by the AsyncSinkWriter and sent
     * to the destination when * the {@code submitRequestEntries} method is
     * invoked.
     */
    private final ElementConverter<InputT, RequestEntryT> elementConverter;


    /**
     * This method specifies how to persist buffered request entries into the
     * destination. It is implemented when support for a new destination is
     * added.
     * <p>
     * The method is invoked with a set of request entries according to the
     * buffering hints (and the valid limits of the destination). The logic then
     * needs to create and execute the request against the destination (ideally
     * by batching together multiple request entries to increase efficiency).
     * The logic also needs to identify individual request entries that were not
     * persisted successfully and resubmit them using the {@code
     * requeueFailedRequestEntry} method.
     * <p>
     * The method returns a future that indicates, once completed, that all
     * request entries that have been passed to the method on invocation have
     * either been successfully persisted in the destination or have been
     * re-queued.
     * <p>
     * During checkpointing, the sink needs to ensure that there are no
     * outstanding in-flight requests. Ie, that all futures returned by this
     * method are completed.
     *
     * @param requestEntries a set of request entries that should be sent to the
     *                       destination
     * @return a future that completes when all request entries have been
     * successfully persisted to the API or were re-queued
     */
    protected abstract CompletableFuture<?> submitRequestEntries(List<RequestEntryT> requestEntries);


    /**
     * Buffer to hold request entries that should be persisted into the
     * destination.
     * <p>
     * A request entry contain all relevant details to make a call to the
     * destination. Eg, for Kinesis Data Streams a request entry contains the
     * payload and partition key.
     * <p>
     * It seems more natural to buffer InputT, ie, the events that should be
     * persisted, rather than RequestEntryT. However, in practice, the response
     * of a failed request call can make it very hard, if not impossible, to
     * reconstruct the original event. It is much easier, to just construct a
     * new (retry) request entry from the response and add that back to the
     * queue for later retry.
     */
    private final Deque<RequestEntryT> bufferedRequestEntries = new ConcurrentLinkedDeque<>();


    /**
     * Tracks all pending async calls that have been executed since the last
     * checkpoint. Calls that already completed (successfully or unsuccessfully)
     * are automatically removed from the queue. Any request entry that was not
     * successfully persisted need to be handled and retried by the logic in
     * {@code submitRequestsToApi}.
     * <p>
     * There is a limit on the number of concurrent (async) requests that can be
     * handled by the client library. This limit is enforced by checking the
     * size of this queue before issuing new requests.
     * <p>
     * To complete a checkpoint, we need to make sure that no requests are in
     * flight, as they may fail, which could then lead to data loss.
     */
    private Queue<CompletableFuture<?>> inFlightRequests = new ConcurrentLinkedQueue<>();


    /**
     * Signals if enough RequestEntryTs have been buffered according to the user
     * specified buffering hints to make a request against the destination. This
     * functionality will be added to the sink interface by means of an
     * additional FLIP.
     *
     * @return a future that will be completed once a request against the
     * destination can be made
     */
    public CompletableFuture<Boolean> isAvailable() {
        boolean isAvailable =
                bufferedRequestEntries.size() < MAX_BUFFERED_REQUESTS_ENTRIES &&
                inFlightRequests.size() < MAX_IN_FLIGHT_REQUESTS;

        return CompletableFuture.completedFuture(isAvailable);
    }



    @Override
    public void write(InputT element, Context context) throws IOException {
        bufferedRequestEntries.offerLast(elementConverter.apply(element, context));

        flush();  // just for testing
    }

    /**
     * The entire request may fail or single request entries that are part of
     * the request may not be persisted successfully, eg, because of network
     * issues or service side throttling. All request entries that failed with
     * transient failures need to be re-queued with this method so that aren't
     * lost and can be retried later.
     * <p>
     * Request entries that are causing the same error in a reproducible manner,
     * eg, ill-formed request entries, must not be re-queued but the error needs
     * to be handled in the logic of {@code submitRequestEntries}. Otherwise
     * these request entries will be retried indefinitely, always causing the
     * same error.
     */
    protected void requeueFailedRequestEntry(RequestEntryT requestEntry) {
        bufferedRequestEntries.offerFirst(requestEntry);
    }




    private static final int BATCH_SIZE = 100;       // just for testing purposes
    private static final int MAX_IN_FLIGHT_REQUESTS = 20;       // just for testing purposes
    private static final int MAX_BUFFERED_REQUESTS_ENTRIES = 1000;       // just for testing purposes

    public AsyncSinkWriter(ElementConverter<InputT, RequestEntryT> elementConverter) {
        this.elementConverter = elementConverter;
    }




    /**
     * just for testing purposes
     */
    public void flush() {
        // TODO: better use min(BATCH_SIZE, queue.size()) ?
        while (bufferedRequestEntries.size() >= BATCH_SIZE) {
             // limit number of concurrent in flight requests
             if (inFlightRequests.size() > 20) {
                 try {
                     logger.info("sleeping for 100 ms");

                     Thread.sleep(100);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
                 continue;
             }

            // create a batch of request entries that should be persisted in the destination
            ArrayList<RequestEntryT> batch = new ArrayList<>(BATCH_SIZE);

            for (int i=0; i<BATCH_SIZE; i++) {
                batch.add(bufferedRequestEntries.remove());
            }

            logger.info("submit requests for {} elements", batch.size());

            // call the destination specific code that actually persists the request entries
            CompletableFuture<?> future = submitRequestEntries(batch);

            // keep track of in flight request
            inFlightRequests.add(future);

            // remove the request from the tracking queue once it competed
            future.whenComplete((response, err) -> {
                inFlightRequests.remove(future);
            });
        }
    }


    /**
     * In flight requests may fail, but they will be retried if the sink is
     * still healthy.
     * <p>
     * To not lose any requests, there cannot be any outstanding in-flight
     * requests when a commit is initialized. To this end, all in-flight
     * requests need to be completed as part of the pre commit by the {@code
     * AsyncSinkCommiter}.
     */
    @Override
    public List<Collection<CompletableFuture<?>>> prepareCommit(boolean flush) throws IOException {
        if (flush) {
            flush();
        }

        logger.info("Prepare commit. {} requests currently in flight.", inFlightRequests.size());

        // reuse current inFlightRequests as commitable and create empty queue to avoid copy and clearing
        List<Collection<CompletableFuture<?>>> committable = Collections.singletonList(inFlightRequests);

        // all in-flight requests are handled by the AsyncSinkCommiter and new elements cannot be added to the queue during a commit, so it's save to create a new queue
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
        return Collections.singletonList(bufferedRequestEntries);
    }



    @Override
    public void close() throws Exception {

    }
}
