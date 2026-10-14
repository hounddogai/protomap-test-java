package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CashPositionValidationEndpoint extends AbstractCashPositionValidationEndpoint {
    public CashPositionValidationEndpoint(CashPositionValidationApplication application) {
        super(application);
    }
}
