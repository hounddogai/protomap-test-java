package com.acme.finance.domains.billing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CreditNoteWorkflowEndpoint extends AbstractCreditNoteWorkflowEndpoint {
    public CreditNoteWorkflowEndpoint(CreditNoteWorkflowApplication application) {
        super(application);
    }
}
