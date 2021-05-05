package com.amazonaws.samples.flink.api.sink;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.util.function.Function;


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

public class GenericApiSink<InputT, RequestT extends Serializable, ResponseT> extends RichSinkFunction<InputT> implements CheckpointedFunction {

    protected ApiRequestBuffer<RequestT, ResponseT> producer;

    protected Function<InputT, RequestT> elementToRequest;



    @Override
    public void invoke(InputT element, Context context) throws Exception {
        producer.queueRequest(elementToRequest.apply(element));
    }

    @Override
    public void close() throws Exception {
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // producer.initiateCheckpoint(); store all events from buffer in state; producer.completeCheckpoint();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // load events from state back into producer queue
    }

}

