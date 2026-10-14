package com.acme.finance.domains.lending;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class LoanApplicationEndpoint extends AbstractLoanApplicationEndpoint {
    public LoanApplicationEndpoint(LoanApplicationApplication application) {
        super(application);
    }
}
