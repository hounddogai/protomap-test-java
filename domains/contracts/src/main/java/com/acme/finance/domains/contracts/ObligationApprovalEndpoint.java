package com.acme.finance.domains.contracts;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ObligationApprovalEndpoint extends AbstractObligationApprovalEndpoint {
    public ObligationApprovalEndpoint(ObligationApprovalApplication application) {
        super(application);
    }
}
