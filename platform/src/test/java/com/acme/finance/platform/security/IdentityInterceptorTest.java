package com.acme.finance.platform.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.RequestContext;
import com.acme.finance.platform.CallIdentity;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserValidationServiceGrpc;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IdentityInterceptorTest {
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> TENANT = Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> HEADER_TENANT = Context.key("header-tenant");
    private final List<String> lookups = new CopyOnWriteArrayList<>();
    private final List<String> served = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

    /** Answers lookups as the Go monolith does: they bear the workload credential, the tenant of their request, and a
     * service account. It answers the record of the id it was asked about, and rejects a revoked token. */
    private final UserValidationServiceGrpc.UserValidationServiceImplBase identity =
            new UserValidationServiceGrpc.UserValidationServiceImplBase() {
                @Override
                public void checkUserEligibility(CheckUserEligibilityRequest request,
                        StreamObserver<CheckUserEligibilityResponse> responses) {
                    RequestContext context = request.getContext();
                    if (!context.getActor().getServiceAccount() || !context.getTenantId().equals(HEADER_TENANT.get())) {
                        responses.onError(Status.UNAUTHENTICATED.withDescription("context is required").asRuntimeException());
                        return;
                    }
                    lookups.add(request.getUserId());
                    String status = request.getUserId().equals("revoked-token") ? "revoked" : "ok";
                    responses.onNext(CheckUserEligibilityResponse.newBuilder()
                            .setUser(User.newBuilder().setUserId(request.getUserId()))
                            .setResult(OperationResult.newBuilder().setStatus(status)).build());
                    responses.onCompleted();
                }
            };

    private final ServerInterceptor workloadOnly = new ServerInterceptor() {
        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                ServerCallHandler<Q, R> next) {
            if (!"Bearer workload-token".equals(headers.get(AUTHORIZATION)) || headers.get(TENANT) == null) {
                call.close(Status.UNAUTHENTICATED.withDescription("missing bearer token or tenant"), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            return Contexts.interceptCall(Context.current().withValue(HEADER_TENANT, headers.get(TENANT)), call,
                    headers, next);
        }
    };

    /** A protected service, whose handler records the credentials its own calls would forward. */
    private final UserValidationServiceGrpc.UserValidationServiceImplBase handler =
            new UserValidationServiceGrpc.UserValidationServiceImplBase() {
                @Override
                public void checkUserEligibility(CheckUserEligibilityRequest request,
                        StreamObserver<CheckUserEligibilityResponse> responses) {
                    served.add(CallIdentity.AUTHORIZATION.get() + " " + CallIdentity.TENANT.get());
                    responses.onNext(CheckUserEligibilityResponse.getDefaultInstance());
                    responses.onCompleted();
                }
            };

    private ManagedChannel serve(String name, io.grpc.ServerServiceDefinition service) throws Exception {
        Server server = InProcessServerBuilder.forName(name).addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        resources.add(() -> channel.shutdownNow());
        resources.add(() -> server.shutdownNow());
        return channel;
    }

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable resource : resources) {
            resource.close();
        }
    }

    private UserValidationServiceGrpc.UserValidationServiceBlockingStub caller(ManagedChannel channel, String token) {
        Metadata headers = new Metadata();
        headers.put(AUTHORIZATION, "Bearer " + token);
        headers.put(TENANT, "tenant-a");
        return UserValidationServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Test
    void resolvesCallersWithTheWorkloadCredentialAndForwardsTheirOwn() throws Exception {
        var lookup = ClientInterceptors.intercept(
                serve("identity", ServerInterceptors.intercept(identity, workloadOnly)), CallIdentity.forwarding());
        var interceptor = new IdentityInterceptor(UserValidationServiceGrpc.newBlockingStub(lookup), "workload-token");
        var protectedChannel = serve("protected", ServerInterceptors.intercept(handler, interceptor));
        var request = CheckUserEligibilityRequest.getDefaultInstance();
        caller(protectedChannel, "caller-token").checkUserEligibility(request);
        assertEquals(List.of("Bearer caller-token tenant-a"), served);
        var revoked = assertThrows(StatusRuntimeException.class,
                () -> caller(protectedChannel, "revoked-token").checkUserEligibility(request));
        assertEquals(Status.Code.PERMISSION_DENIED, revoked.getStatus().getCode());
        var anonymous = assertThrows(StatusRuntimeException.class,
                () -> UserValidationServiceGrpc.newBlockingStub(protectedChannel).checkUserEligibility(request));
        assertEquals(Status.Code.UNAUTHENTICATED, anonymous.getStatus().getCode());
        assertEquals(List.of("caller-token", "revoked-token"), lookups);
        assertEquals(1, served.size());
    }
}
