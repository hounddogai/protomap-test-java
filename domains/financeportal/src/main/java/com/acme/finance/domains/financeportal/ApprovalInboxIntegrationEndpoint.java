package com.acme.finance.domains.financeportal;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ApprovalInboxIntegrationEndpoint extends AbstractApprovalInboxIntegrationEndpoint {
    public ApprovalInboxIntegrationEndpoint(ApprovalInboxIntegrationApplication application) {
        super(application);
    }
}
