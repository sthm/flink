package com.amazonaws.samples.flink.api.sink;


import com.amazonaws.samples.flink.api.sink.committer.ApiCommitter;
import com.amazonaws.samples.flink.api.sink.committer.ApiSinkCommittable;
import com.amazonaws.samples.flink.api.sink.writer.ApiWriterState;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * The main design goal is to obtain a generic sink that implements the common functionality required, such as,
 * buffering/batching events and retry capabilities. The sink should be easily extensible and provide reasonable
 * semantics, ie, at-least once semantics.
 *
 * The sink implements the interface of a SinkFunction and hands over requests to a service specific AwsProducer,
 * that actually sends the requests to the sink.
 *
 * Limitations:
 *  - breaks ordering of events during reties
 *  - does not support exactly-once semantics
 */
public abstract class ApiSink<InputT, RequestT extends Serializable, ResponseT> implements Sink<InputT, ApiSinkCommittable<ResponseT>, ApiWriterState<RequestT>, Void> {
    @Override
    public Optional<Committer<ApiSinkCommittable<ResponseT>>> createCommitter() throws IOException {
        return Optional.of(new ApiCommitter<>());
    }

    @Override
    public Optional<GlobalCommitter<ApiSinkCommittable<ResponseT>, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<ApiSinkCommittable<ResponseT>>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<ApiWriterState<RequestT>>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
