package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BankTransferLifecycleEndpoint extends AbstractBankTransferLifecycleEndpoint {
    public BankTransferLifecycleEndpoint(BankTransferLifecycleApplication application) {
        super(application);
    }
}
