package com.acme.finance.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.acme.common.v1.Actor;
import com.acme.common.v1.RequestContext;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PlatformTest {
    private RequestContext context() {
        return RequestContext.newBuilder().setTenantId("tenant-a").setTraceId("trace-a")
                .setActor(Actor.newBuilder().setUserId("user-a").addRoles("finance").addRoles("approver"))
                .putBaggage("source", "month-close").build();
    }

    @Test
    void copiesNestedRepeatedAndMapFieldsThroughReflection() {
        assertEquals(context(), BeanCopies.copy(context(), RequestContext.class));
        assertEquals(context().getActor(), BeanCopies.copy(context().getActor(), Actor.class));
    }

    @Test
    void preservesJsonBytesAndAnyRoundTrips() {
        var value = context();
        assertEquals(value, JsonCodec.decode(JsonCodec.encode(value), value, RequestContext.class));
        assertEquals(value, RpcSupport.unpack(Any.pack(value), RequestContext.class));
        assertThrows(StatusRuntimeException.class, () -> RpcSupport.unpack(Any.pack(value), Actor.class));
    }

    @Test
    void validatesTenantAndActorSeparately() {
        var missingTenant = assertThrows(StatusRuntimeException.class,
                () -> RpcSupport.validate(RequestContext.getDefaultInstance()));
        assertEquals(Status.Code.INVALID_ARGUMENT, missingTenant.getStatus().getCode());
        var missingActor = assertThrows(StatusRuntimeException.class,
                () -> RpcSupport.validate(RequestContext.newBuilder().setTenantId("tenant").build()));
        assertEquals(Status.Code.UNAUTHENTICATED, missingActor.getStatus().getCode());
        RpcSupport.validate(context());
    }

    @Test
    void boundsRetriesAndPreservesNonRetryableStatus() {
        var attempts = new AtomicInteger();
        assertEquals("done", RpcSupport.retryRead(() -> {
            if (attempts.incrementAndGet() < 3) { throw Status.UNAVAILABLE.asRuntimeException(); }
            return "done";
        }));
        assertEquals(3, attempts.get());
        var failure = assertThrows(StatusRuntimeException.class,
                () -> RpcSupport.retryRead(() -> { throw Status.PERMISSION_DENIED.asRuntimeException(); }));
        assertEquals(Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    }

    @Test
    void terminatesObserversExactlyOnce() {
        List<String> signals = new ArrayList<>();
        StreamObserver<String> observer = new StreamObserver<>() {
            public void onNext(String value) { signals.add(value); }
            public void onCompleted() { signals.add("completed"); }
            public void onError(Throwable failure) { signals.add(Status.fromThrowable(failure).getCode().name()); }
        };
        RpcSupport.unary(observer, () -> "value");
        assertEquals(List.of("value", "completed"), signals);
        signals.clear();
        RpcSupport.unary(observer, () -> { throw Status.NOT_FOUND.asRuntimeException(); });
        assertEquals(List.of("NOT_FOUND"), signals);
    }

    @Test
    void observesStreamingCompletionAndFailure() {
        var collector = new StreamCollector<>("empty");
        collector.onNext("first");
        collector.onNext("last");
        collector.onCompleted();
        assertEquals("last", collector.await());
        var failed = new StreamCollector<>("empty");
        failed.onError(Status.DATA_LOSS.asRuntimeException());
        assertEquals(Status.Code.DATA_LOSS,
                assertThrows(StatusRuntimeException.class, failed::await).getStatus().getCode());
    }
}
