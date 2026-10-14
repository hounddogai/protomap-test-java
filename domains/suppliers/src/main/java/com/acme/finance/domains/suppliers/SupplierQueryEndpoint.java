package com.acme.finance.domains.suppliers;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class SupplierQueryEndpoint extends AbstractSupplierQueryEndpoint {
    public SupplierQueryEndpoint(SupplierQueryApplication application) {
        super(application);
    }
}
