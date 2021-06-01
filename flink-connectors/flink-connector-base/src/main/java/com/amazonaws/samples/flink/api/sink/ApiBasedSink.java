package com.amazonaws.samples.flink.api.sink;


import com.amazonaws.samples.flink.api.sink.committer.ApiBasedSinkCommitter;
import com.amazonaws.samples.flink.api.sink.committer.ApiBasedSinkCommittable;
import com.amazonaws.samples.flink.api.sink.writer.ApiBasedSinkWriterState;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * The main design goal is to obtain a generic sink that implements the common functionality required, such as,
 * buffering/batching events and retry capabilities. The design biases on extensibility to be applicable to a broad
 * coverage of different API endpoints.
 *
 * Limitations:
 *  - breaks ordering of events during reties
 *  - does not support exactly-once semantics
 */
public abstract class ApiBasedSink<InputT, RequestT extends Serializable> implements Sink<InputT, ApiBasedSinkCommittable, ApiBasedSinkWriterState<RequestT>, Void> {
    @Override
    public Optional<Committer<ApiBasedSinkCommittable>> createCommitter() throws IOException {
        return Optional.of(new ApiBasedSinkCommitter());
    }

    @Override
    public Optional<GlobalCommitter<ApiBasedSinkCommittable, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<ApiBasedSinkCommittable>> getCommittableSerializer() {
        return Optional.of(new SimpleVersionedSerializer<ApiBasedSinkCommittable>() {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(ApiBasedSinkCommittable completableFutures) throws IOException {
                return new byte[0];
            }

            @Override
            public ApiBasedSinkCommittable deserialize(int i, byte[] bytes) throws IOException {
                return null;
            }
        });
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<ApiBasedSinkWriterState<RequestT>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
