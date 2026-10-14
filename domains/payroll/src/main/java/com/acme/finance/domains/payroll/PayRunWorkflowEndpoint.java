package com.acme.finance.domains.payroll;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PayRunWorkflowEndpoint extends AbstractPayRunWorkflowEndpoint {
    public PayRunWorkflowEndpoint(PayRunWorkflowApplication application) {
        super(application);
    }
}
