package com.acme.finance.domains.payroll;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class DeductionLifecycleEndpoint extends AbstractDeductionLifecycleEndpoint {
    public DeductionLifecycleEndpoint(DeductionLifecycleApplication application) {
        super(application);
    }
}
