package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimLifecycleEndpoint extends AbstractClaimLifecycleEndpoint {
    public ClaimLifecycleEndpoint(ClaimLifecycleApplication application) {
        super(application);
    }
}
