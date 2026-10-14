package com.acme.finance.domains.expenses;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ReceiptWorkflowEndpoint extends AbstractReceiptWorkflowEndpoint {
    public ReceiptWorkflowEndpoint(ReceiptWorkflowApplication application) {
        super(application);
    }
}
