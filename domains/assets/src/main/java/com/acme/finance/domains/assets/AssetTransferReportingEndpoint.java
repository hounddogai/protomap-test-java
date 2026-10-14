package com.acme.finance.domains.assets;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class AssetTransferReportingEndpoint extends AbstractAssetTransferReportingEndpoint {
    public AssetTransferReportingEndpoint(AssetTransferReportingApplication application) {
        super(application);
    }
}
