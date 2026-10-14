package com.acme.finance.domains.payroll;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PayStubValidationEndpoint extends AbstractPayStubValidationEndpoint {
    public PayStubValidationEndpoint(PayStubValidationApplication application) {
        super(application);
    }
}
