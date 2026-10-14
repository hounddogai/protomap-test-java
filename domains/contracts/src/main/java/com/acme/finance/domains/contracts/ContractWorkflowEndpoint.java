package com.acme.finance.domains.contracts;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ContractWorkflowEndpoint extends AbstractContractWorkflowEndpoint {
    public ContractWorkflowEndpoint(ContractWorkflowApplication application) {
        super(application);
    }
}
