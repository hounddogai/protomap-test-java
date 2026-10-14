package com.acme.finance.domains.lending;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class LoanWorkflowEndpoint extends AbstractLoanWorkflowEndpoint {
    public LoanWorkflowEndpoint(LoanWorkflowApplication application) {
        super(application);
    }
}
