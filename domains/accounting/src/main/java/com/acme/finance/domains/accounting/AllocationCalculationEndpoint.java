package com.acme.finance.domains.accounting;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AllocationCalculationEndpoint extends AbstractAllocationCalculationEndpoint {
    public AllocationCalculationEndpoint(AllocationCalculationApplication application) {
        super(application);
    }
}
