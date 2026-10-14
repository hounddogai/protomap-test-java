package com.acme.finance.domains.lending;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class LoanApplicationCalculationEndpoint extends AbstractLoanApplicationCalculationEndpoint {
    public LoanApplicationCalculationEndpoint(LoanApplicationCalculationApplication application) {
        super(application);
    }
}
