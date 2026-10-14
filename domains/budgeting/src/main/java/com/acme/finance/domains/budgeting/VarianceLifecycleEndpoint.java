package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class VarianceLifecycleEndpoint extends AbstractVarianceLifecycleEndpoint {
    public VarianceLifecycleEndpoint(VarianceLifecycleApplication application) {
        super(application);
    }
}
