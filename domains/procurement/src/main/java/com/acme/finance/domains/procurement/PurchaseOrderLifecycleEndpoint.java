package com.acme.finance.domains.procurement;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PurchaseOrderLifecycleEndpoint extends AbstractPurchaseOrderLifecycleEndpoint {
    public PurchaseOrderLifecycleEndpoint(PurchaseOrderLifecycleApplication application) {
        super(application);
    }
}
