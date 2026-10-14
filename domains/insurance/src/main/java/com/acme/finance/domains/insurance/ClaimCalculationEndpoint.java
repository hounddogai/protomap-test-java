package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimCalculationEndpoint extends AbstractClaimCalculationEndpoint {
    public ClaimCalculationEndpoint(ClaimCalculationApplication application) {
        super(application);
    }
}
