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
 * A generic sink for destinations that provide an async client to persist
 * data.
 * <p>
 * The design of the sink focuses on extensibility and a broad support of
 * destinations. The core of the sink is kept generic and free of any connector
 * specific dependencies. The sink is designed to participate in checkpointing
 * to provide at-least once semantics, but it is limited to destinations that
 * provide a client that supports async requests.
 * <p>
 * Limitations:
 * <ul>
 *   <li>The sink is designed for destinations that provide an async client. If you cannot persist data into the sink in an async fashion, you cannot leverage the sink.</li>
 *   <li>The sink tries to persist InputTs in the order they are added to the sink, but reorderings may still occur, eg, when RequestEntryTs need to be retried.</li>
 *   <li>We are not considering support for exactly-once semantics at this point.</li>
 * </ul>
 */
public abstract class AsyncSink<InputT, RequestEntryT extends Serializable> implements Sink<InputT, Collection<CompletableFuture<?>>, Collection<RequestEntryT>, Void> {
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
        // FIXME: return Optional.empty(); causes a runtime exception

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
    public Optional<SimpleVersionedSerializer<Collection<RequestEntryT>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
