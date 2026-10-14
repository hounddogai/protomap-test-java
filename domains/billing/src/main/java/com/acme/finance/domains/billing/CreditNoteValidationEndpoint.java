package com.acme.finance.domains.billing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CreditNoteValidationEndpoint extends AbstractCreditNoteValidationEndpoint {
    public CreditNoteValidationEndpoint(CreditNoteValidationApplication application) {
        super(application);
    }
}
