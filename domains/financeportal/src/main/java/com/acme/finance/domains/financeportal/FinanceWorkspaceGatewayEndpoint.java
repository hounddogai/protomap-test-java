package com.acme.finance.domains.financeportal;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class FinanceWorkspaceGatewayEndpoint extends AbstractFinanceWorkspaceGatewayEndpoint {
    public FinanceWorkspaceGatewayEndpoint(FinanceWorkspaceGatewayApplication application) {
        super(application);
    }
}
