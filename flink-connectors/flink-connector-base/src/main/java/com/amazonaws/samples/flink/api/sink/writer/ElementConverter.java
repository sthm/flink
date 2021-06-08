package com.amazonaws.samples.flink.api.sink.writer;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.Serializable;

/**
 * This interface specifies the mapping between elements of a stream to request
 * entries that can be sent to the destination. The mapping is provided by the
 * end-user of a sink, not the sink creator.
 * <p>
 * The request entries contain all relevant information required to create and
 * sent the actual request. Eg, for Kinesis Data Streams, the request entry
 * includes the payload and the partition key.
 */
public interface ElementConverter<InputT, RequestEntryT> extends Serializable {
    RequestEntryT apply(InputT element, SinkWriter.Context context);
}
