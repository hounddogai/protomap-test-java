package com.acme.finance.domains.invoicing;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class InvoiceTemplateWorkflowEndpoint extends AbstractInvoiceTemplateWorkflowEndpoint {
    public InvoiceTemplateWorkflowEndpoint(InvoiceTemplateWorkflowApplication application) {
        super(application);
    }
}
