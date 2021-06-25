package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;


public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable> implements SinkWriter<InputT, Collection<CompletableFuture<?>>, Collection<RequestEntryT>> {

    static final Logger logger = LogManager.getLogger(AsyncSinkWriter.class);


    /**
     * The ElementConverter provides a  mapping between for the elements of a
     * stream to request entries that can be sent to the destination.
     * <p>
     * The resulting request entry is buffered by the AsyncSinkWriter and sent
     * to the destination when the {@code submitRequestEntries} method is
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
    private final BlockingQueue<RequestEntryT> bufferedRequestEntries = new LinkedBlockingDeque<>(MAX_BUFFERED_REQUESTS_ENTRIES);


    /**
     * Buffer to hold request entries that need to be retired, eg, because of
     * throttling applied at the destination.
     * <p>
     * As retries should be rare, this is a non-blocking queue (in contrast to
     * {@code bufferedRequestEntries}). In this way, requests that need to be
     * retried can quickly be added back to the internal buffer without blocking
     * further processing. Moreover, in this way the implementation of {@code
     * requeueFailedRequestEntry} does not have to deal with {@code
     * InterruptedException}.
     * <p>
     * Having a separate queue for retires allows to preserve the order of
     * retries based on the time the corresponding request failed.
     * <p>
     * As only failed request entries are added to the queue, the size of the
     * queue is effectively bound by {@code MAX_BUFFERED_REQUESTS_ENTRIES} *
     * {@code MAX_IN_FLIGHT_REQUESTS}.
     */
    private final Queue<RequestEntryT> failedRequestEntries = new ConcurrentLinkedQueue<>();


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
    private BlockingQueue<CompletableFuture<?>> inFlightRequests = new LinkedBlockingDeque<>(MAX_IN_FLIGHT_REQUESTS);



    @Override
    public void write(InputT element, Context context) throws IOException {
        try {
            // blocks if too many elements have been buffered
            bufferedRequestEntries.put(elementConverter.apply(element, context));

            // blocks if too many async requests are in flight
            flush();
        } catch (InterruptedException e) {
            // FIXME: add exception to signature instead of swallowing it
            Thread.currentThread().interrupt();
        }
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
        failedRequestEntries.add(requestEntry);
    }




    private static final int MAX_BATCH_SIZE = 50;       // just for testing purposes
    private static final int MAX_IN_FLIGHT_REQUESTS = 5;       // just for testing purposes
    private static final int MAX_BUFFERED_REQUESTS_ENTRIES = 1000;       // just for testing purposes

    public AsyncSinkWriter(ElementConverter<InputT, RequestEntryT> elementConverter) {
        this.elementConverter = elementConverter;
    }


    /**
     * Persists buffered RequestsEntries into the destination by invoking {@code
     * submitRequestEntries} with batches according to the user specified
     * buffering hints.
     *
     * The method blocks if too many async requests are in flight.
     */
    private void flush() throws InterruptedException {
        while (bufferedRequestEntries.size()+ failedRequestEntries.size() >= MAX_BATCH_SIZE) {

            // create a batch of request entries that should be persisted in the destination
            ArrayList<RequestEntryT> batch = new ArrayList<>(MAX_BATCH_SIZE);

            // prioritise retry events queued in failedRequestEntries to minimize latency for retries
            while (batch.size() <= MAX_BATCH_SIZE && !failedRequestEntries.isEmpty()) {
                try {
                    batch.add(failedRequestEntries.remove());
                } catch (NoSuchElementException e) {
                    // if there are not enough failedRequestEntries elements, add elements from bufferedRequestEntries
                    break;
                }
            }

            while (batch.size() <= MAX_BATCH_SIZE && !bufferedRequestEntries.isEmpty()) {
                try {
                    batch.add(bufferedRequestEntries.remove());
                } catch (NoSuchElementException e) {
                    // if there are not enough elements, just create a smaller batch
                    break;
                }
            }

            // call the destination specific code that actually persists the request entries
            CompletableFuture<?> future = submitRequestEntries(batch);

            // keep track of in flight request; block if too many requests are in flight
            inFlightRequests.put(future);

            // remove the request from the tracking queue once it competed
            future.whenComplete((response, err) -> inFlightRequests.remove(future));
        }
    }


    /**
     * In flight requests will be retried if the sink is still healthy. But if
     * in-flight requests fail after a checkpoint has been triggered and Flink
     * needs to recover from the checkpoint, the (failed) in-flight requests are
     * gone and cannot be retried. Hence, there cannot be any outstanding
     * in-flight requests when a commit is initialized.
     * <p>
     * To this end, all in-flight requests need to be passed to the {@code
     * AsyncSinkCommiter} in order to be completed as part of the pre commit.
     */
    @Override
    public List<Collection<CompletableFuture<?>>> prepareCommit(boolean flush) throws IOException {
        logger.info("Prepare commit. {} requests currently in flight.", inFlightRequests.size());

        // reuse current inFlightRequests as commitable and create empty queue to avoid copy and clearing
        List<Collection<CompletableFuture<?>>> committable = Collections.singletonList(inFlightRequests);

        // all in-flight requests are handled by the AsyncSinkCommiter and new elements cannot be added to the queue during a commit, so it's save to create a new queue
        inFlightRequests = new LinkedBlockingDeque<>();

        return committable;
    }

    /**
     * All in-flight requests that are relevant for the snapshot have been
     * completed, but there may still be request entries in the internal buffers
     * that are yet to be sent to the endpoint. These request entries are stored
     * in the snapshot state so that they don't get lost in case of a
     * failure/restart of the application.
     */
    @Override
    public List<Collection<RequestEntryT>> snapshotState() throws IOException {
        return Arrays.asList(failedRequestEntries, bufferedRequestEntries);
    }



    @Override
    public void close() throws Exception {

    }
}
