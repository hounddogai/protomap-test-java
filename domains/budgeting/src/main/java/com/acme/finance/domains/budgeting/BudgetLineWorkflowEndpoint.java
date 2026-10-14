package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BudgetLineWorkflowEndpoint extends AbstractBudgetLineWorkflowEndpoint {
    public BudgetLineWorkflowEndpoint(BudgetLineWorkflowApplication application) {
        super(application);
    }
}
