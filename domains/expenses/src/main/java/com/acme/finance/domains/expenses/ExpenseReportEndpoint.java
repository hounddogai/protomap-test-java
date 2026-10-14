package com.acme.finance.domains.expenses;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ExpenseReportEndpoint extends AbstractExpenseReportEndpoint {
    public ExpenseReportEndpoint(ExpenseReportApplication application) {
        super(application);
    }
}
