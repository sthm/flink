package org.apache.flink.connector.base.sink.writer;


public interface ResultFuture {
    /**
     * Completes the result future.
     */
    void complete();

    /**
     * Completes the result future exceptionally with an exception.
     *
     * @param error A Throwable object.
     */
    void completeExceptionally(Throwable error);
}
