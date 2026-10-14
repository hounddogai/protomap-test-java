package com.acme.finance.domains.procurement;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class GoodsReceiptApprovalEndpoint extends AbstractGoodsReceiptApprovalEndpoint {
    public GoodsReceiptApprovalEndpoint(GoodsReceiptApprovalApplication application) {
        super(application);
    }
}
