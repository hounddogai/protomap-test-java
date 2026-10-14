package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class LiquidityForecastCalculationEndpoint extends AbstractLiquidityForecastCalculationEndpoint {
    public LiquidityForecastCalculationEndpoint(LiquidityForecastCalculationApplication application) {
        super(application);
    }
}
