package com.amazonaws.samples.flink.api.sink;


import com.amazonaws.samples.flink.api.sink.committer.AsyncSinkCommitter;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The main design goal is to obtain a generic sink that implements the common functionality required, such as,
 * buffering/batching events and retry capabilities. The design biases on extensibility to be applicable to a broad
 * coverage of different API endpoints.
 *
 * Limitations:
 *  - breaks ordering of events during reties
 *  - does not support exactly-once semantics
 */
public abstract class AsyncSink<InputT, RequestT extends Serializable> implements Sink<InputT, Collection<CompletableFuture<?>>, Collection<RequestT>, Void> {
    @Override
    public Optional<Committer<Collection<CompletableFuture<?>>>> createCommitter() throws IOException {
        return Optional.of(new AsyncSinkCommitter());
    }

    @Override
    public Optional<GlobalCommitter<Collection<CompletableFuture<?>>, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<CompletableFuture<?>>>> getCommittableSerializer() {
        return Optional.of(new SimpleVersionedSerializer<Collection<CompletableFuture<?>>>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(Collection<CompletableFuture<?>> completableFutures) throws IOException {
                return new byte[0];
            }

            @Override
            public Collection<CompletableFuture<?>> deserialize(int i, byte[] bytes) throws IOException {
                return null;
            }
        });
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<RequestT>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
