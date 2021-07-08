/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements SinkWriter<InputT, Void, Collection<RequestEntryT>> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncSinkWriter.class);

    private int inFlightRequestsCount;
    private final MailboxExecutor mailboxExecutor;
    private final Sink.ProcessingTimeService timeService;

    private static final int MAX_BATCH_SIZE = 50; // just for testing purposes
    private static final int MAX_IN_FLIGHT_REQUESTS = 1; // just for testing purposes
    private static final int MAX_BUFFERED_REQUESTS_ENTRIES = 1000; // just for testing purposes

    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter, Sink.InitContext context) {
        this.elementConverter = elementConverter;
        this.mailboxExecutor = context.getMailboxExecutor();
        this.timeService = context.getProcessingTimeService();
    }

    /**
     * The ElementConverter provides a mapping between for the elements of a stream to request
     * entries that can be sent to the destination.
     *
     * <p>The resulting request entry is buffered by the AsyncSinkWriter and sent to the destination
     * when the {@code submitRequestEntries} method is invoked.
     */
    private final ElementConverter<InputT, RequestEntryT> elementConverter;

    /**
     * This method specifies how to persist buffered request entries into the destination. It is
     * implemented when support for a new destination is added.
     *
     * <p>The method is invoked with a set of request entries according to the buffering hints (and
     * the valid limits of the destination). The logic then needs to create and execute the request
     * against the destination (ideally by batching together multiple request entries to increase
     * efficiency). The logic also needs to identify individual request entries that were not
     * persisted successfully and resubmit them using the {@code requeueFailedRequestEntry} method.
     *
     * <p>During checkpointing, the sink needs to ensure that there are no outstanding in-flight
     * requests.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param requestResult a ResultFuture that needs to be completed once all request entries that
     *     have been passed to the method on invocation have either been successfully persisted in
     *     the destination or have been re-queued through {@code requeueFailedRequestEntry}
     */
    protected abstract void submitRequestEntries(
            List<RequestEntryT> requestEntries, ResultFuture<RequestEntryT> requestResult);

    /**
     * Buffer to hold request entries that should be persisted into the destination.
     *
     * <p>A request entry contain all relevant details to make a call to the destination. Eg, for
     * Kinesis Data Streams a request entry contains the payload and partition key.
     *
     * <p>It seems more natural to buffer InputT, ie, the events that should be persisted, rather
     * than RequestEntryT. However, in practice, the response of a failed request call can make it
     * very hard, if not impossible, to reconstruct the original event. It is much easier, to just
     * construct a new (retry) request entry from the response and add that back to the queue for
     * later retry.
     */
    private final Queue<RequestEntryT> bufferedRequestEntries = new ArrayDeque<>();

    /**
     * Buffer to hold request entries that need to be retired, eg, because of throttling applied at
     * the destination.
     *
     * <p>As retries should be rare, this is a non-blocking queue (in contrast to {@code
     * bufferedRequestEntries}). In this way, requests that need to be retried can quickly be added
     * back to the internal buffer without blocking further processing. Moreover, in this way the
     * implementation of {@code requeueFailedRequestEntry} does not have to deal with {@code
     * InterruptedException}.
     *
     * <p>Having a separate queue for retires allows to preserve the order of retries based on the
     * time the corresponding request failed.
     *
     * <p>Only failed request entries are added to the queue and requests entries in this queue are
     * chosen over request entries from {@code bufferedRequestEntries} when the next request is made
     * against the destination. So the size of the queue is effectively bound by {@code
     * MAX_BUFFERED_REQUESTS_ENTRIES} * {@code MAX_IN_FLIGHT_REQUESTS}.
     */
    private final Queue<RequestEntryT> failedRequestEntries = new ArrayDeque<>();

    /**
     * Tracks all pending async calls that have been executed since the last checkpoint. Calls that
     * completed (successfully or unsuccessfully) are automatically decrementing the counter. Any
     * request entry that was not successfully persisted needs to be handled and retried by the
     * logic in {@code submitRequestsToApi}.
     *
     * <p>There is a limit on the number of concurrent (async) requests that can be handled by the
     * client library. This limit is enforced by checking the queue size before accepting a new
     * element into the queue.
     *
     * <p>To complete a checkpoint, we need to make sure that no requests are in flight, as they may
     * fail, which could then lead to data loss.
     */
    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        // blocks if too many elements have been buffered
        while (bufferedRequestEntries.size() >= MAX_BUFFERED_REQUESTS_ENTRIES) {
            mailboxExecutor.yield();
        }

        bufferedRequestEntries.add(elementConverter.apply(element, context));

        // blocks if too many async requests are in flight
        flush();
    }

    /**
     * Persists buffered RequestsEntries into the destination by invoking {@code
     * submitRequestEntries} with batches according to the user specified buffering hints.
     *
     * <p>The method blocks if too many async requests are in flight.
     */
    private void flush() throws InterruptedException {
        while (bufferedRequestEntries.size() + failedRequestEntries.size() >= MAX_BATCH_SIZE) {

            // create a batch of request entries that should be persisted in the destination
            ArrayList<RequestEntryT> batch = new ArrayList<>(MAX_BATCH_SIZE);

            // prioritise retry events queued in failedRequestEntries to minimize latency for
            // retries
            while (batch.size() <= MAX_BATCH_SIZE && !failedRequestEntries.isEmpty()) {
                try {
                    batch.add(failedRequestEntries.remove());
                } catch (NoSuchElementException e) {
                    // if there are not enough failedRequestEntries elements, add elements from
                    // bufferedRequestEntries
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

            ResultFuture<RequestEntryT> requestResult =
                    new ResultFuture<>() {
                        @Override
                        public void complete(Collection<RequestEntryT> failedRequestEntries) {
                            mailboxExecutor.execute(
                                    () -> completeRequest(failedRequestEntries),
                                    "Complete request and requeue %d failed request entries",
                                    failedRequestEntries.size());
                        }
                    };

            while (inFlightRequestsCount >= MAX_IN_FLIGHT_REQUESTS) {
                mailboxExecutor.yield();
            }

            inFlightRequestsCount++;
            submitRequestEntries(batch, requestResult);
        }
    }

    private void completeRequest(Collection<RequestEntryT> failedRequestEntries) {
        inFlightRequestsCount--;
        this.failedRequestEntries.addAll(failedRequestEntries);
    }

    /**
     * In flight requests will be retried if the sink is still healthy. But if in-flight requests
     * fail after a checkpoint has been triggered and Flink needs to recover from the checkpoint,
     * the (failed) in-flight requests are gone and cannot be retried. Hence, there cannot be any
     * outstanding in-flight requests when a commit is initialized.
     *
     * <p>To this end, all in-flight requests need to be passed to the {@code AsyncSinkCommiter} in
     * order to be completed as part of the pre commit.
     */
    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException, InterruptedException {
        if (flush) {
            flush();
        }

        // wait until all in-flight requests completed
        while (inFlightRequestsCount > 0) {
            mailboxExecutor.yield();
        }

        return Collections.emptyList();
    }

    /**
     * All in-flight requests that are relevant for the snapshot have been completed, but there may
     * still be request entries in the internal buffers that are yet to be sent to the endpoint.
     * These request entries are stored in the snapshot state so that they don't get lost in case of
     * a failure/restart of the application.
     */
    @Override
    public List<Collection<RequestEntryT>> snapshotState() throws IOException {
        return Arrays.asList(failedRequestEntries, bufferedRequestEntries);
    }

    @Override
    public void close() throws Exception {}
}
