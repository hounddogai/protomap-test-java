package com.acme.finance.domains.suppliers;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class SupplierContractApprovalEndpoint extends AbstractSupplierContractApprovalEndpoint {
    public SupplierContractApprovalEndpoint(SupplierContractApprovalApplication application) {
        super(application);
    }
}
