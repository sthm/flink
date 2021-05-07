package com.amazonaws.samples.flink.api.sink.committer;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ApiCommitter<ResponseT> implements Committer<ApiSinkCommittable<ResponseT>> {
    @Override
    public List<ApiSinkCommittable<ResponseT>> commit(List<ApiSinkCommittable<ResponseT>> committables) throws IOException {

        for (ApiSinkCommittable<ResponseT> committable : committables) {
            committable.forEach(CompletableFuture::join);
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }
}
