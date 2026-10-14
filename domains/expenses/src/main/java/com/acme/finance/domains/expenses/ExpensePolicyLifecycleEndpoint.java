package com.acme.finance.domains.expenses;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ExpensePolicyLifecycleEndpoint extends AbstractExpensePolicyLifecycleEndpoint {
    public ExpensePolicyLifecycleEndpoint(ExpensePolicyLifecycleApplication application) {
        super(application);
    }
}
