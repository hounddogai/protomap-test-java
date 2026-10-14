package com.acme.finance.domains.assets;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class FixedAssetApprovalEndpoint extends AbstractFixedAssetApprovalEndpoint {
    public FixedAssetApprovalEndpoint(FixedAssetApprovalApplication application) {
        super(application);
    }
}
