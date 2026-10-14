package com.acme.finance.platform.security;

import com.acme.common.v1.Actor;
import com.acme.common.v1.RequestContext;
import com.acme.finance.platform.CallIdentity;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.UserValidationServiceGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;
import org.springframework.beans.factory.annotation.Value;

/** Checks the caller with identity before dispatching either unary or streaming requests. */
@GrpcGlobalServerInterceptor
public final class IdentityInterceptor implements ServerInterceptor {
    public static final Context.Key<String> SUBJECT = Context.key("finance-subject");
    private static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> TENANT = Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER);
    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identity;
    private final String serviceToken;

    public IdentityInterceptor(UserValidationServiceGrpc.UserValidationServiceBlockingStub identity, @Value("${ACME_SERVICE_TOKEN:}") String serviceToken) {
        this.identity = identity;
        this.serviceToken = serviceToken;
    }

    @Override
    public <Req, Res> ServerCall.Listener<Req> interceptCall(ServerCall<Req, Res> call, Metadata headers,
            ServerCallHandler<Req, Res> next) {
        String authorization = headers.get(AUTHORIZATION);
        String tenant = headers.get(TENANT);
        if (authorization == null || !authorization.startsWith("Bearer ") || authorization.length() == 7
                || tenant == null || tenant.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("bearer token and tenant are required"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        String token = authorization.substring(7);
        try {
            var request = CheckUserEligibilityRequest.newBuilder()
                    .setUserId(token)
                    .setContext(RequestContext.newBuilder().setTenantId(tenant)
                            .setActor(Actor.newBuilder().setUserId("finance").setServiceAccount(true)))
                    .build();
            // The lookup is this server's own call, which the forwarding interceptor signs with its credential.
            Context lookup = Context.current().withValue(CallIdentity.AUTHORIZATION, "Bearer " + serviceToken)
                    .withValue(CallIdentity.TENANT, tenant);
            Context previous = lookup.attach();
            CheckUserEligibilityResponse decision;
            try {
                decision = identity.withDeadlineAfter(3, TimeUnit.SECONDS).checkUserEligibility(request);
            } finally {
                lookup.detach(previous);
            }
            if (!"ok".equals(decision.getResult().getStatus())
                    || !token.equals(decision.getUser().getUserId())) {
                call.close(Status.PERMISSION_DENIED.withDescription("identity rejected the caller"), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            var context = Context.current().withValue(SUBJECT, decision.getUser().getUserId())
                    .withValue(CallIdentity.AUTHORIZATION, authorization).withValue(CallIdentity.TENANT, tenant);
            return Contexts.interceptCall(context, call, headers, next);
        } catch (StatusRuntimeException failure) {
            call.close(Status.UNAUTHENTICATED.withDescription("identity check failed").withCause(failure), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }
}
