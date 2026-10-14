package com.acme.finance.domains.procurement;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PurchaseOrderQueryEndpoint extends AbstractPurchaseOrderQueryEndpoint {
    public PurchaseOrderQueryEndpoint(PurchaseOrderQueryApplication application) {
        super(application);
    }
}
