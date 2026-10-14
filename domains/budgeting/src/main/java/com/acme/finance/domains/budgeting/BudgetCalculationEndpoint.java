package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BudgetCalculationEndpoint extends AbstractBudgetCalculationEndpoint {
    public BudgetCalculationEndpoint(BudgetCalculationApplication application) {
        super(application);
    }
}
