package com.acme.finance.domains.insurance;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ClaimApprovalEndpoint extends AbstractClaimApprovalEndpoint {
    public ClaimApprovalEndpoint(ClaimApprovalApplication application) {
        super(application);
    }
}
