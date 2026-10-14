package com.acme.finance.domains.assets;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class DepreciationCalculationEndpoint extends AbstractDepreciationCalculationEndpoint {
    public DepreciationCalculationEndpoint(DepreciationCalculationApplication application) {
        super(application);
    }
}
