package com.amazonaws.samples.flink.api.sink.writer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class ApiBasedSinkWriterState<RequestT extends Serializable> extends ArrayList<RequestT> {
    public ApiBasedSinkWriterState(Collection<? extends RequestT> c) {
        super(c);
    }
}
