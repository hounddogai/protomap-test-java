package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimValidationEndpoint extends AbstractClaimValidationEndpoint {
    public ClaimValidationEndpoint(ClaimValidationApplication application) {
        super(application);
    }
}
