package com.acme.finance.domains.procurement;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class PurchaseRequisitionValidationEndpoint extends AbstractPurchaseRequisitionValidationEndpoint {
    public PurchaseRequisitionValidationEndpoint(PurchaseRequisitionValidationApplication application) {
        super(application);
    }
}
