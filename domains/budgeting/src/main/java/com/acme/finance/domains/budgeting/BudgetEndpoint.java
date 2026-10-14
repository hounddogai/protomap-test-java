package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BudgetEndpoint extends AbstractBudgetEndpoint {
    public BudgetEndpoint(BudgetApplication application) {
        super(application);
    }
}
