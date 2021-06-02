package com.amazonaws.samples.flink.api.sink.committer;

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
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }
}
