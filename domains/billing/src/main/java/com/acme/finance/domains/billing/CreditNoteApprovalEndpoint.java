package com.acme.finance.domains.billing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CreditNoteApprovalEndpoint extends AbstractCreditNoteApprovalEndpoint {
    public CreditNoteApprovalEndpoint(CreditNoteApprovalApplication application) {
        super(application);
    }
}
