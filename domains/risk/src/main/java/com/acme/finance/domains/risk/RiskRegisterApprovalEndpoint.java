package com.acme.finance.domains.risk;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class RiskRegisterApprovalEndpoint extends AbstractRiskRegisterApprovalEndpoint {
    public RiskRegisterApprovalEndpoint(RiskRegisterApprovalApplication application) {
        super(application);
    }
}
