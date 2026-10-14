package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class FxHedgeApprovalEndpoint extends AbstractFxHedgeApprovalEndpoint {
    public FxHedgeApprovalEndpoint(FxHedgeApprovalApplication application) {
        super(application);
    }
}
