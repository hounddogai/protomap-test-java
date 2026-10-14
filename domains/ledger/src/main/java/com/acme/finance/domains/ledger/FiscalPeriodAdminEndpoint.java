package com.acme.finance.domains.ledger;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class FiscalPeriodAdminEndpoint extends AbstractFiscalPeriodAdminEndpoint {
    public FiscalPeriodAdminEndpoint(FiscalPeriodAdminApplication application) {
        super(application);
    }
}
