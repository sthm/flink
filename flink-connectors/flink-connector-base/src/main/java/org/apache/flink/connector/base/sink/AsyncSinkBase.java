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

package org.apache.flink.connector.base.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.sink.committer.AsyncSinkCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Semaphore;

/**
 * A generic sink for destinations that provide an async client to persist data.
 *
 * <p>The design of the sink focuses on extensibility and a broad support of destinations. The core
 * of the sink is kept generic and free of any connector specific dependencies. The sink is designed
 * to participate in checkpointing to provide at-least once semantics, but it is limited to
 * destinations that provide a client that supports async requests.
 *
 * <p>Limitations:
 *
 * <ul>
 *   <li>The sink is designed for destinations that provide an async client. Destinations that
 *       cannot ingest events in an async fashion cannot be supported by the sink.
 *   <li>The sink usually persist InputTs in the order they are added to the sink, but reorderings
 *       may occur, eg, when RequestEntryTs need to be retried.
 *   <li>We are not considering support for exactly-once semantics at this point.
 * </ul>
 */
public abstract class AsyncSinkBase<InputT, RequestEntryT extends Serializable>
        implements Sink<InputT, Void, Collection<RequestEntryT>, Void> {

    @Override
    public Optional<Committer<Void>> createCommitter(CommitterInitContext context)
            throws IOException {
        return Optional.of(new AsyncSinkCommitter(context));
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter(
            CommitterInitContext context) throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        // FIXME: return Optional.empty(); causes a runtime exception

        return Optional.of(
                new SimpleVersionedSerializer<>() {
                    @Override
                    public int getVersion() {
                        return 0;
                    }

                    @Override
                    public byte[] serialize(Void completableFutures) throws IOException {
                        return new byte[0];
                    }

                    @Override
                    public Void deserialize(int i, byte[] bytes) throws IOException {
                        return null;
                    }
                });
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Collection<RequestEntryT>>>
            getWriterStateSerializer() {
        return Optional.empty();
    }
}
