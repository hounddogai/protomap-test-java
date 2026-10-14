package com.acme.finance.domains.payroll;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class TaxWithholdingApprovalEndpoint extends AbstractTaxWithholdingApprovalEndpoint {
    public TaxWithholdingApprovalEndpoint(TaxWithholdingApprovalApplication application) {
        super(application);
    }
}
