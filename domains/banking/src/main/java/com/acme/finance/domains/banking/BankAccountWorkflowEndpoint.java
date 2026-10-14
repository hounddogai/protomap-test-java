package com.acme.finance.domains.banking;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BankAccountWorkflowEndpoint extends AbstractBankAccountWorkflowEndpoint {
    public BankAccountWorkflowEndpoint(BankAccountWorkflowApplication application) {
        super(application);
    }
}
