package com.acme.finance.domains.expenses;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ExpenseReportCalculationEndpoint extends AbstractExpenseReportCalculationEndpoint {
    public ExpenseReportCalculationEndpoint(ExpenseReportCalculationApplication application) {
        super(application);
    }
}
