package org.apache.flink.connector.base.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AsyncSinkCommitter implements Committer<Collection<CompletableFuture<?>>> {
    static final Logger logger = LogManager.getLogger(AsyncSinkCommitter.class);

    @Override
    public List<Collection<CompletableFuture<?>>> commit(List<Collection<CompletableFuture<?>>> committables) throws IOException {

        for (Collection<CompletableFuture<?>> committable : committables) {
            if (committable == null) {
                continue;
            }

            logger.info("Committing. Waiting for {} in-flight requests to complete.", committable.size());

            committable.forEach(CompletableFuture::join);

            // this function receives a reference to the original bufferedRequestEntries, so when all futures completed, the list should be empty
            // (modulo race conditions where a completed future is just about to be removed)
            assert(committables.isEmpty());
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }
}
