package org.apache.flink.connector.base.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class AsyncSinkCommitter implements Committer<Semaphore> {
    static final Logger logger = LogManager.getLogger(AsyncSinkCommitter.class);

    private final int maxInFlightRequests;

    public AsyncSinkCommitter(int maxInFlightRequests) {
        this.maxInFlightRequests = maxInFlightRequests;
    }

    @Override
    public List<Semaphore> commit(List<Semaphore> committables) throws IOException {

        for (Semaphore committable : committables) {
            if (committable == null) {
                continue;
            }

            logger.info("Committing. Waiting for {} in-flight requests to complete.", maxInFlightRequests - committable.availablePermits());

            try {
                // wait until all outstanding in flight requests have been completed
                committable.acquire(maxInFlightRequests);
            } catch (InterruptedException e) {
                // FIXME: add exception to signature instead of swallowing it; requires change to Flink API
                Thread.currentThread().interrupt();
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }
}
