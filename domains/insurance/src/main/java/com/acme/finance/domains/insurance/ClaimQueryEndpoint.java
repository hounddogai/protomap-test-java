package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimQueryEndpoint extends AbstractClaimQueryEndpoint {
    public ClaimQueryEndpoint(ClaimQueryApplication application) {
        super(application);
    }
}
