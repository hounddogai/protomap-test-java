package com.acme.finance.platform;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Observes a finite import or reconciliation stream and keeps its final acknowledgment. */
public final class StreamCollector<T> implements StreamObserver<T> {
    private final CompletableFuture<T> completed = new CompletableFuture<>();
    private T latest;

    public StreamCollector(T empty) {
        latest = empty;
    }

    @Override
    public void onNext(T value) {
        latest = value;
    }

    @Override
    public void onError(Throwable failure) {
        completed.completeExceptionally(failure);
    }

    @Override
    public void onCompleted() {
        completed.complete(latest);
    }

    public T await() {
        try {
            return completed.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withCause(failure).asRuntimeException();
        } catch (java.util.concurrent.TimeoutException failure) {
            throw Status.DEADLINE_EXCEEDED.withCause(failure).asRuntimeException();
        } catch (java.util.concurrent.ExecutionException failure) {
            throw RpcSupport.status(failure);
        }
    }
}
