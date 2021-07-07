package org.apache.flink.connector.base.sink.writer;

import java.util.Collection;

public interface ResultFuture<RequestEntryT> {
    /**
     * Completes the result future.
     *
     * @param failedRequestEntries Request entries that need to be retried at a later point
     */
    void complete(Collection<RequestEntryT> failedRequestEntries);
}
