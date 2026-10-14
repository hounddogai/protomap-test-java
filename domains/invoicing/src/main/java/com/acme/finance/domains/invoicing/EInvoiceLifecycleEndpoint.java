package com.acme.finance.domains.invoicing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class EInvoiceLifecycleEndpoint extends AbstractEInvoiceLifecycleEndpoint {
    public EInvoiceLifecycleEndpoint(EInvoiceLifecycleApplication application) {
        super(application);
    }
}
