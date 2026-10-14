package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class FxHedgeSyncEndpoint extends AbstractFxHedgeSyncEndpoint {
    public FxHedgeSyncEndpoint(FxHedgeSyncApplication application) {
        super(application);
    }
}
