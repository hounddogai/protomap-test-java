package com.acme.finance.platform;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/** Carries the authenticated caller through nested RPCs and context-aware executors. */
public final class CallIdentity {
    public static final Context.Key<String> AUTHORIZATION = Context.key("finance-authorization");
    public static final Context.Key<String> TENANT = Context.key("finance-tenant");

    private CallIdentity() {}

    public static ClientInterceptor forwarding() {
        return new ClientInterceptor() {
            @Override
            public <Req, Res> ClientCall<Req, Res> interceptCall(MethodDescriptor<Req, Res> method,
                    CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
                    @Override
                    public void start(Listener<Res> listener, Metadata headers) {
                        if (AUTHORIZATION.get() != null) {
                            headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                                    AUTHORIZATION.get());
                        }
                        if (TENANT.get() != null) {
                            headers.put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), TENANT.get());
                        }
                        super.start(listener, headers);
                    }
                };
            }
        };
    }
}
