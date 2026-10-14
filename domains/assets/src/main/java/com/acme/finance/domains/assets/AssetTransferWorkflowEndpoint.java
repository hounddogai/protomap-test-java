package com.acme.finance.domains.assets;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AssetTransferWorkflowEndpoint extends AbstractAssetTransferWorkflowEndpoint {
    public AssetTransferWorkflowEndpoint(AssetTransferWorkflowApplication application) {
        super(application);
    }
}
