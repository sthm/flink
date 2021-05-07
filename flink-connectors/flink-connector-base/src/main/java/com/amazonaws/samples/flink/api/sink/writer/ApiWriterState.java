package com.amazonaws.samples.flink.api.sink.writer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class ApiWriterState<RequestT extends Serializable> extends ArrayList<RequestT> {
    public ApiWriterState(Collection<? extends RequestT> c) {
        super(c);
    }
}
