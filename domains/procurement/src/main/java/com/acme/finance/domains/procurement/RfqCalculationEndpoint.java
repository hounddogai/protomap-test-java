package com.acme.finance.domains.procurement;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class RfqCalculationEndpoint extends AbstractRfqCalculationEndpoint {
    public RfqCalculationEndpoint(RfqCalculationApplication application) {
        super(application);
    }
}
