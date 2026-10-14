package com.acme.finance.domains.billing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CreditNoteLifecycleEndpoint extends AbstractCreditNoteLifecycleEndpoint {
    public CreditNoteLifecycleEndpoint(CreditNoteLifecycleApplication application) {
        super(application);
    }
}
