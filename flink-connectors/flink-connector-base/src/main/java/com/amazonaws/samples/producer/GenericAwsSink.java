package com.amazonaws.samples.producer;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import software.amazon.awssdk.core.SdkClient;




/**
 * The sink buffers records and issues batchRequests
 * Limitations:
 *  - does not respect ordering of events
 */

public class GenericAwsSink<InputT, ClientT extends SdkClient, RequestT, ResponseT> extends RichSinkFunction<InputT> implements CheckpointedFunction {

    protected GenericAwsProducer<InputT, ClientT, RequestT, ResponseT> producer;

    @Override
    public void invoke(InputT element, Context context) throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}

