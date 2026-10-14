package com.acme.finance.platform;

import com.acme.common.v1.RequestContext;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/** Shared deadlines, status translation, and observer lifecycle rules for the finance domains. */
public final class RpcSupport {
    private RpcSupport() {}

    public static void validate(RequestContext context) {
        if (context.getTenantId().isBlank()) {
            throw Status.INVALID_ARGUMENT.withDescription("context.tenant_id is required").asRuntimeException();
        }
        if (context.getActor().getUserId().isBlank() && !context.getActor().getServiceAccount()) {
            throw Status.UNAUTHENTICATED.withDescription("context.actor is required").asRuntimeException();
        }
        if (Context.current().isCancelled()) {
            throw Status.CANCELLED.withDescription("request cancelled").asRuntimeException();
        }
    }

    public static <Req, Res> Res call(Req request, Function<Req, Res> operation) {
        return operation.apply(request);
    }

    /** Retry only reads; callers must explicitly opt in after checking the operation is idempotent. */
    public static <T> T retryRead(Supplier<T> operation) {
        for (int attempt = 0; ; attempt++) {
            try {
                return operation.get();
            } catch (StatusRuntimeException failure) {
                if (attempt == 2 || failure.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                    throw failure;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(20L << attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw Status.CANCELLED.withCause(interrupted).asRuntimeException();
                }
            }
        }
    }

    /**
     * An observer of a unary call that completes {@code completion} with its reply or its failure, so that whoever
     * makes the call need not wait for it, as the event listeners must not.
     */
    public static <T> StreamObserver<T> completing(CompletableFuture<T> completion) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                completion.complete(value);
            }

            @Override
            public void onError(Throwable failure) {
                completion.completeExceptionally(failure);
            }

            @Override
            public void onCompleted() {
                completion.completeExceptionally(Status.INTERNAL.withDescription("the call ended without a reply")
                        .asRuntimeException());
            }
        };
    }

    public static <T> T await(ListenableFuture<T> future) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withCause(failure).asRuntimeException();
        } catch (TimeoutException failure) {
            future.cancel(true);
            throw Status.DEADLINE_EXCEEDED.withCause(failure).asRuntimeException();
        } catch (ExecutionException failure) {
            throw status(failure.getCause());
        }
    }

    public static StatusRuntimeException status(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        if (failure instanceof StatusRuntimeException known) {
            return known;
        }
        if (failure instanceof StatusException known) {
            return known.getStatus().asRuntimeException();
        }
        if (failure instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED.withCause(failure).asRuntimeException();
        }
        if (failure instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(failure.getMessage()).withCause(failure).asRuntimeException();
        }
        return Status.INTERNAL.withDescription("operation failed").withCause(failure).asRuntimeException();
    }

    public static <T> void unary(StreamObserver<T> observer, Supplier<T> operation) {
        final T result;
        try {
            result = operation.get();
        } catch (RuntimeException failure) {
            observer.onError(status(failure));
            return;
        }
        observer.onNext(result);
        observer.onCompleted();
    }

    public static <T extends Message> T unpack(Any value, Class<T> type) {
        try {
            return value.unpack(type);
        } catch (InvalidProtocolBufferException failure) {
            throw Status.INVALID_ARGUMENT.withDescription("unexpected envelope type").withCause(failure)
                    .asRuntimeException();
        }
    }
}
