package com.acme.finance.platform;

import java.util.function.Function;

/** An outbound application port; gRPC method references supply its transport adapter. */
@FunctionalInterface
public interface RpcPort<Req, Res> {
    Res exchange(Req request);

    static <Req, Res> RpcPort<Req, Res> adapt(Function<Req, Res> transport) {
        return transport::apply;
    }
}
