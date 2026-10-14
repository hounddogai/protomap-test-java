package com.acme.finance.domains.lending;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class CollateralValidationEndpoint extends AbstractCollateralValidationEndpoint {
    public CollateralValidationEndpoint(CollateralValidationApplication application) {
        super(application);
    }
}
