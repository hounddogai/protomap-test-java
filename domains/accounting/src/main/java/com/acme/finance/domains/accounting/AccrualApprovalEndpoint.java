package com.acme.finance.domains.accounting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AccrualApprovalEndpoint extends AbstractAccrualApprovalEndpoint {
    public AccrualApprovalEndpoint(AccrualApprovalApplication application) {
        super(application);
    }
}
