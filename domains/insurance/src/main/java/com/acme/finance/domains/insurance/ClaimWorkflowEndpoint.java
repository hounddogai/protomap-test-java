package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimWorkflowEndpoint extends AbstractClaimWorkflowEndpoint {
    public ClaimWorkflowEndpoint(ClaimWorkflowApplication application) {
        super(application);
    }
}
