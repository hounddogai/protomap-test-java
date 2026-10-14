package com.acme.finance.domains.treasury;

import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public final class BankTransferQueryEndpoint extends AbstractBankTransferQueryEndpoint {
    public BankTransferQueryEndpoint(BankTransferQueryApplication application) {
        super(application);
    }
}
