package com.acme.finance.domains.billing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CreditNoteCalculationEndpoint extends AbstractCreditNoteCalculationEndpoint {
    public CreditNoteCalculationEndpoint(CreditNoteCalculationApplication application) {
        super(application);
    }
}
