package com.acme.finance.domains.invoicing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class InvoiceBatchApprovalEndpoint extends AbstractInvoiceBatchApprovalEndpoint {
    public InvoiceBatchApprovalEndpoint(InvoiceBatchApprovalApplication application) {
        super(application);
    }
}
