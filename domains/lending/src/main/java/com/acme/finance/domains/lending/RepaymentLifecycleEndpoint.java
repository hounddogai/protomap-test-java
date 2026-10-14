package com.acme.finance.domains.lending;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class RepaymentLifecycleEndpoint extends AbstractRepaymentLifecycleEndpoint {
    public RepaymentLifecycleEndpoint(RepaymentLifecycleApplication application) {
        super(application);
    }
}
