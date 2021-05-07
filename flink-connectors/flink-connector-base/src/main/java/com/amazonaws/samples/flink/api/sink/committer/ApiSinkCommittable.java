package com.amazonaws.samples.flink.api.sink.committer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks all currently in-flight requests
 */
public class ApiSinkCommittable<ResponseT> extends ArrayList<CompletableFuture<ResponseT>> {
    public ApiSinkCommittable(Collection<? extends CompletableFuture<ResponseT>> c) {
        super(c);
    }
}
