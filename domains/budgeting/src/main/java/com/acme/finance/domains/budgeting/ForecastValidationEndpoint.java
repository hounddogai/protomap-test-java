package com.acme.finance.domains.budgeting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class ForecastValidationEndpoint extends AbstractForecastValidationEndpoint {
    public ForecastValidationEndpoint(ForecastValidationApplication application) {
        super(application);
    }
}
