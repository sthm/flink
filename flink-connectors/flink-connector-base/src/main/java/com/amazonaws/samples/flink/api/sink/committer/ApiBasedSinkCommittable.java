package com.amazonaws.samples.flink.api.sink.committer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks all currently in-flight requests
 */
public class ApiBasedSinkCommittable extends ArrayList<CompletableFuture<?>> {
    public ApiBasedSinkCommittable(Collection<? extends CompletableFuture<?>> c) {
        super(c);
    }
}
