package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BudgetLineReportingEndpoint extends AbstractBudgetLineReportingEndpoint {
    public BudgetLineReportingEndpoint(BudgetLineReportingApplication application) {
        super(application);
    }
}
