package com.acme.finance.domains.risk;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class StressTestValidationEndpoint extends AbstractStressTestValidationEndpoint {
    public StressTestValidationEndpoint(StressTestValidationApplication application) {
        super(application);
    }
}
